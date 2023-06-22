package com.scalableminds.zarrjava.store;

import com.squareup.okhttp.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class HttpStore implements Store {

    @Nonnull
    private final OkHttpClient httpClient;
    @Nonnull
    private final String path;

    public HttpStore(@Nonnull String path) {
        this.httpClient = new OkHttpClient();
        this.path = path;
    }

    @Nullable
    ByteBuffer get(Request request) {
        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            try (ResponseBody body = response.body()) {
                return ByteBuffer.wrap(body.bytes());
            }
        } catch (IOException e) {
            return null;
        }
    }

    @Nullable
    @Override
    public ByteBuffer get(String key) {
        Request request = new Request.Builder().url(path + "/" + key).build();
        return get(request);
    }

    @Nullable
    @Override
    public ByteBuffer get(String key, long start) {
        Request request = new Request.Builder().url(path + "/" + key).header("Range",
                start < 0 ? String.format("Bytes=%d", start) : String.format("Bytes=%d-", start)).build();

        return get(request);
    }

    @Nullable
    @Override
    public ByteBuffer get(String key, long start, long end) {
        assert start >= 0;
        Request request = new Request.Builder().url(path + "/" + key).header("Range",
                String.format("Bytes=%d-%d", start, end + 1)).build();
        return get(request);
    }

    @Override
    public void set(String key, ByteBuffer bytes) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void delete(String key) {
        throw new UnsupportedOperationException("Not implemented");
    }


    @Override
    public String toString() {
        return path;
    }
}
