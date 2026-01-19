package dev.zarr.zarrjava.store;

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
}
