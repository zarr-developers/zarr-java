package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Array;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.core.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class MemoryStoreTest extends WritableStoreTest {



    @Override
    Store writableStore() {
        return new MemoryStore();
    }

    @Override
    StoreHandle storeHandleWithData() {
        StoreHandle memoryStoreHandle = new MemoryStore().resolve();
        memoryStoreHandle.set(ByteBuffer.wrap(testData()));
        return memoryStoreHandle;
    }
}
