package dev.zarr.zarrjava.store;

import com.squareup.okhttp.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class HttpStore implements Store {

    @Nonnull
    private final OkHttpClient httpClient;
    @Nonnull
    private final String uri;

    public HttpStore(@Nonnull String uri) {
        this.httpClient = new OkHttpClient();
        this.uri = uri;
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
    ByteBuffer get(Request request) {
        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            if (!response.isSuccessful()) {
                return null;
            }
            try (ResponseBody body = response.body()) {
                if (body == null) {
                    return null;
                }
                return ByteBuffer.wrap(body.bytes());
            }
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public boolean exists(String[] keys) {
        Request request = new Request.Builder().head().url(resolveKeys(keys)).build();
        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            return response.isSuccessful();
        } catch (IOException e) {
            return false;
        }
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys) {
        Request request = new Request.Builder().url(resolveKeys(keys)).build();
        return get(request);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start) {
        Request request = new Request.Builder().url(resolveKeys(keys)).header(
                        "Range", start < 0 ? String.format("bytes=%d", start) : String.format("bytes=%d-", start))
                .build();

        return get(request);
    }

    @Nullable
    @Override
    public ByteBuffer get(String[] keys, long start, long end) {
        if (start < 0) {
            throw new IllegalArgumentException("Argument 'start' needs to be non-negative.");
        }
        Request request = new Request.Builder().url(resolveKeys(keys)).header(
                "Range", String.format("bytes=%d-%d", start, end - 1)).build();
        return get(request);
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
        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            if (!response.isSuccessful()) {
                return null;
            }
            ResponseBody body = response.body();
            if (body == null) return null;
            InputStream stream = body.byteStream();

            // Ensure closing the stream also closes the response
            return new FilterInputStream(stream) {
                @Override
                public void close() throws IOException {
                    super.close();
                    body.close();
                }
            };
        } catch (IOException e) {
            return null;
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

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
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
}
