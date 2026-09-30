package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.ZarrException;

import java.io.IOException;
import java.nio.ByteBuffer;

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

    @Override
    StoreHandle storeHandleWithoutData() {
        return new MemoryStore().resolve();
    }

    @Override
    Store storeWithArrays() throws ZarrException, IOException {
        MemoryStore memoryStore = new MemoryStore();
        writeTestGroupV3(memoryStore.resolve("array"), false);
        return memoryStore;
    }
}
