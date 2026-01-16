package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Array;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class HttpStoreTest extends StoreTest {

    @Override
    StoreHandle storeHandleWithData() {
        HttpStore httpStore = new dev.zarr.zarrjava.store.HttpStore("https://uk1s3.embassy.ebi.ac.uk/idr/zarr/v0.5/idr0033A");
        return httpStore.resolve("BR00109990_C2.zarr", "0", "0");

    }

    @Test
    public void testHttpStore() throws IOException, ZarrException {
        Array array = Array.open(storeHandleWithData());
        Assertions.assertArrayEquals(new long[]{5, 1552, 2080}, array.metadata().shape);
    }

    @Override
    @Test
    public void testStoreGetSize() {
        // size is not defined in BR00109990_C2.zarr
        long size = storeHandleWithData().getSize();
        Assertions.assertEquals(-1, size);
    }

    @Override
    void testList() throws ZarrException, IOException {
        // listing is not supported in HttpStore
    }
}
