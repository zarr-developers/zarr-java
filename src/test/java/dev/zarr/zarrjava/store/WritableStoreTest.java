package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Array;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.core.Group;
import dev.zarr.zarrjava.core.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class WritableStoreTest extends StoreTest {
    abstract Store writableStore();

    @Test
    public void testList() throws IOException, ZarrException {
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

        List<String> allKeys = storeHandle.list()
                .map(node -> String.join("/", node))
                .collect(Collectors.toList());
        Assertions.assertEquals(21, allKeys.size(), "Total number of keys in store should be 21 but was: " + allKeys);
    }

    @Test
    public void testWriteRead() throws IOException, ZarrException {
        StoreHandle storeHandle = writableStore().resolve("testWriteRead");
        boolean useParallel = true;
        Group group = writeTestGroupV3(storeHandle, useParallel);
        assertIsTestGroupV3(group, useParallel);
    }

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testWriteReadV3(boolean useParallel) throws ZarrException, IOException {
        int[] testData = testDataInt();
        Store store = writableStore();
        StoreHandle storeHandle = store.resolve("testWriteReadV3").resolve(store.getClass().getSimpleName());

        dev.zarr.zarrjava.v3.Group group = dev.zarr.zarrjava.v3.Group.create(storeHandle);
        Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT32)
                .withChunkShape(64, 64)
        );
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1024, 1024}, testData), useParallel);
        group.createGroup("subgroup");
        group.setAttributes(new Attributes(b -> b.set("some", "value")));
        Stream<Node> nodes = group.list();
        Assertions.assertEquals(2, nodes.count());

        ucar.ma2.Array result = array.read(useParallel);
        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
        Attributes attrs = group.metadata().attributes;
        Assertions.assertNotNull(attrs);
        Assertions.assertEquals("value", attrs.getString("some"));
    }

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testMemoryStoreV2(boolean useParallel) throws ZarrException, IOException {
        int[] testData = testDataInt();
        Store store = writableStore();
        StoreHandle storeHandle = store.resolve("testMemoryStoreV2").resolve(store.getClass().getSimpleName());

        dev.zarr.zarrjava.v2.Group group = dev.zarr.zarrjava.v2.Group.create(storeHandle);
        dev.zarr.zarrjava.v2.Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT32)
                .withChunks(512, 512)
        );
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1024, 1024}, testData), useParallel);
        group.createGroup("subgroup");
        Stream<dev.zarr.zarrjava.core.Node> nodes = group.list();
        group.setAttributes(new Attributes().set("description", "test group"));
        Assertions.assertEquals(2, nodes.count());

        ucar.ma2.Array result = array.read(useParallel);
        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
        Attributes attrs = group.metadata().attributes;
        Assertions.assertNotNull(attrs);
        Assertions.assertEquals("test group", attrs.getString("description"));
    }

}
