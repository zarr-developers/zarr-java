package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Array;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class HttpStoreTest extends StoreTest {

    @Override
    StoreHandle storeHandleWithData() {
        return br00109990StoreHandle().resolve("c", "0", "0", "0");
    }

    @Override
    StoreHandle storeHandleWithoutData() {
        return br00109990StoreHandle().resolve("nonexistent", "path", "to", "data");
    }

    @Override
    Store storeWithArrays() {
        return br00109990StoreHandle().store;
    }

    StoreHandle br00109990StoreHandle() {
        HttpStore httpStore = new dev.zarr.zarrjava.store.HttpStore("https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.5/idr0033A");
        return httpStore.resolve("BR00109990_C2.zarr", "0", "0");
    }

    @Test
    public void testOpen() throws IOException, ZarrException {
        Array array = Array.open(br00109990StoreHandle());
        Assertions.assertArrayEquals(new long[]{5, 1552, 2080}, array.metadata().shape);
    }

    @Override
    @Test
    @Disabled("List is not supported in HttpStore")
    public void testList() {
    }

    @Override
    @Test
    @Disabled("List is not supported in HttpStore")
    public void testListedItemsExist() {
    }

    @Override
    @Test
    @Disabled("List is not supported in HttpStore")
    public void testListChildren() {
    }


}
