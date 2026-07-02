package dev.zarr.zarrjava.store;

import okhttp3.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;

public class HttpStore implements Store {

    @Nonnull
    private final OkHttpClient httpClient;
    @Nonnull
    private final String uri;

    public HttpStore(@Nonnull String uri) {
        this(uri, 60, 3, 1000);
    }

    public HttpStore(@Nonnull String uri, int timeoutSeconds, int maxRetries, long retryDelayMs) {
        this.uri = uri;
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofSeconds(timeoutSeconds))
                .readTimeout(Duration.ofSeconds(timeoutSeconds))
                .addInterceptor(new RetryInterceptor(maxRetries, retryDelayMs))
                .build();
    }

    String resolveKeys(String[] keys) {
        HttpUrl url = HttpUrl.parse(uri);
        if (url == null) {
            throw new IllegalArgumentException("Invalid base URI: " + uri);
        }
        HttpUrl.Builder builder = url.newBuilder();
        for (String key : keys) {
            for (String segment : key.split("/", -1)) {
                builder.addPathSegment(segment);
            }
        }
        return builder.build().toString();
    }

    @Nullable
    ByteBuffer get(Request request, String[] keys) {
        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                if (response.code() == 404) {
                    return null;
                }
                throw StoreException.readFailed(
                        this.toString(),
                        keys,
                        new IOException("HTTP request failed with status code: " + response.code() + " " + response.message()));
            }
            ResponseBody body = response.body();
            return (body == null) ? null : ByteBuffer.wrap(body.bytes());
        } catch (IOException e) {
            throw StoreException.readFailed(this.toString(), keys, e);
        }
    }

    @Override
    public boolean exists(String[] keys) {
        Request request = new Request.Builder().head().url(resolveKeys(keys)).build();
        try (Response response = httpClient.newCall(request).execute()) {
            return response.isSuccessful();
        } catch (IOException e) {
            return false;
        }
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys) {
        Request request = new Request.Builder().url(resolveKeys(keys)).build();
        return get(request, keys);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start) {
        Request request = new Request.Builder().url(resolveKeys(keys)).header(
                        "Range", start < 0 ? String.format("bytes=%d", start) : String.format("bytes=%d-", start))
                .build();

        return get(request, keys);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start, long end) {
        if (start < 0) {
            throw new IllegalArgumentException("Argument 'start' needs to be non-negative.");
        }
        Request request = new Request.Builder().url(resolveKeys(keys)).header(
                "Range", String.format("bytes=%d-%d", start, end - 1)).build();
        return get(request, keys);
    }

    @Override
    public void set(String[] keys, ByteBuffer bytes) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void delete(String[] keys) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Nonnull
    @Override
    public StoreHandle resolve(String... keys) {
        return new StoreHandle(this, keys);
    }

    @Override
    public String toString() {
        return uri;
    }

    @Override
    @Nullable
    public InputStream getInputStream(String[] keys, long start, long end) {
        if (start < 0) {
            throw new IllegalArgumentException("Argument 'start' needs to be non-negative.");
        }
        Request request = new Request.Builder().url(resolveKeys(keys)).header(
                "Range", String.format("bytes=%d-%d", start, end - 1)).build();

        try {
            // We do NOT use try-with-resources here because the stream must remain open
            Response response = httpClient.newCall(request).execute();
            if (!response.isSuccessful()) {
                if (response.code() == 404) {
                    response.close();
                    return null;
                }
                int code = response.code();
                String msg = response.message();
                response.close();
                throw StoreException.readFailed(this.toString(), keys,
                        new IOException("HTTP request failed with status code: " + code + " " + msg));
            }

            ResponseBody body = response.body();
            if (body == null) {
                response.close();
                return null;
            }

            return new FilterInputStream(body.byteStream()) {
                @Override
                public void close() throws IOException {
                    super.close();
                    response.close(); // Closes both body and underlying connection
                }
            };
        } catch (IOException e) {
            throw StoreException.readFailed(this.toString(), keys, e);
        }
    }

    @Override
    public long getSize(String[] keys) {
        String url = resolveKeys(keys);
        // Explicitly request "identity" encoding to prevent OkHttp from adding "gzip"
        // and subsequently stripping the Content-Length header.
        Request request = new Request.Builder()
                .head()
                .url(url)
                .header("Accept-Encoding", "identity")
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                return -1;
            }

            String contentLength = response.header("Content-Length");
            if (contentLength != null) {
                return Long.parseLong(contentLength);
            }
            return -1;
        } catch (NumberFormatException e) {
            throw StoreException.readFailed(
                    this.toString(),
                    keys,
                    new IOException("Invalid Content-Length header value from: " + url, e));
        } catch (IOException e) {
            throw StoreException.readFailed(
                    this.toString(),
                    keys,
                    new IOException("Failed to get content length from HTTP HEAD request to: " + url, e));
        }
    }

    /**
     * Internal interceptor to handle retries for all HttpStore requests.
     */
    private static class RetryInterceptor implements Interceptor {
        private final int maxRetries;
        private final long delay;

        RetryInterceptor(int maxRetries, long delay) {
            this.maxRetries = maxRetries;
            this.delay = delay;
        }

        @Override
        @Nonnull
        public Response intercept(@Nonnull Chain chain) throws IOException {
            Request request = chain.request();
            IOException lastException = null;

            for (int i = 0; i <= maxRetries; i++) {
                try {
                    if (i > 0) Thread.sleep(delay);
                    Response response = chain.proceed(request);

                    // Retry on common transient server errors (502, 503, 504)
                    if (response.isSuccessful() || response.code() == 404 || i == maxRetries || response.code() < 500) {
                        return response;
                    }
                    response.close();
                } catch (IOException e) {
                    lastException = e;
                    if (i == maxRetries) throw e;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Retry interrupted", e);
                }
            }
            throw lastException != null ? lastException : new IOException("Request failed after retries");
        }
    }
}