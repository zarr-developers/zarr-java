package dev.zarr.zarrjava.store;

import com.squareup.okhttp.Call;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import dev.zarr.zarrjava.indexing.OpenSlice;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class HttpStore extends Store {

    private final OkHttpClient httpClient;
    private final String path;

    public HttpStore(String path) {
        this.httpClient = new OkHttpClient();
        this.path = path;
    }

    private String getRangeHeader(OpenSlice byteRange) {
        if (byteRange.start != null) {
            if (byteRange.start < 0) {
                return String.format("bytes=-%d", -byteRange.start);
            } else if (byteRange.end != null && byteRange.end > 0) {
                return String.format("bytes=%d-%d", byteRange.start, byteRange.end + 1);
            } else {
                return String.format("bytes=%d", byteRange.start);
            }
        }
        throw new UnsupportedOperationException("Unsupported range request");
    }

    @Override
    public Optional<byte[]> get(String key, OpenSlice byteRange) {
        Request.Builder builder = new Request.Builder()
                .url(path + "/" + key);

        if (byteRange != null) {
            builder.header("Range", getRangeHeader(byteRange));
        }

        Request request = builder.build();

        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            return Optional.of(response.body().bytes());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public void set(String key, byte[] bytes, OpenSlice byteRange) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void delete(String key) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<String> list(String key) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String toString() {
        return path;
    }
}
