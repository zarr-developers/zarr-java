package dev.zarr.zarrjava.store;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class Store {
    public class ByteRange {
        public Long start;
        public Long end;
    }
    public final class MultiGetRequest {
        public String key;
        public ByteRange byteRange;
    }

    public final class MultiSetRequest {
        public String key;
        public byte[] bytes;
        public ByteRange byteRange;
    }

    public List<Optional<byte[]>> multiGet(List<MultiGetRequest> requests) {
        return requests.stream().map(request -> this.get(request.key, request.byteRange)).collect(Collectors.toList());
    }

    public abstract Optional<byte[]> get(String key, ByteRange byteRange);

    public void multiSet(List<MultiSetRequest> requests) {
        requests.forEach(request -> this.set(request.key, request.bytes, request.byteRange));
    }

    public abstract void set(String key, byte[] bytes, ByteRange byteRange);

    public abstract void delete(String key);

    public abstract List<String> list(String key);
}
