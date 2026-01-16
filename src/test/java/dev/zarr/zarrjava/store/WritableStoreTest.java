package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Group;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class WritableStoreTest extends StoreTest {
    abstract Store writableStore();

    @Test
    public void testList() throws ZarrException, IOException {
        StoreHandle storeHandle = writableStore().resolve("testList");
        boolean useParallel = true;
        writeTestGroupV3(storeHandle, useParallel);
        java.util.Set<String> expectedSubgroupKeys = new java.util.HashSet<>(Arrays.asList(
                "array/c/1/1",
                "array/c/0/0",
                "array/c/0/1",
                "zarr.json",
                "array",
                "array/c/1/0",
                "array/c/1",
                "array/c/0",
                "array/zarr.json",
                "array/c"
        ));

        java.util.Set<String> actualKeys = storeHandle.resolve("subgroup").list()
                .map(node -> String.join("/", node))
                .collect(Collectors.toSet());
        Assertions.assertEquals(expectedSubgroupKeys, actualKeys);
    }

    @Test
    public void testWriteRead() throws IOException, ZarrException {
        StoreHandle storeHandle = writableStore().resolve("testWriteRead");
        boolean useParallel = true;
        Group group = writeTestGroupV3(storeHandle, useParallel);
        assertIsTestGroupV3(group, useParallel);
    }

}
