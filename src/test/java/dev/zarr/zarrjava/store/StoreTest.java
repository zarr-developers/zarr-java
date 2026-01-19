package dev.zarr.zarrjava.store;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.core.Array;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.core.Group;
import dev.zarr.zarrjava.core.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import ucar.ma2.DataType;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class StoreTest extends ZarrTest {

    /**
     * Returns a StoreHandle with some test data written to it (optimally not written by the same Store implementation).
     */
    abstract StoreHandle storeHandleWithData();

    abstract StoreHandle storeHandleWithoutData();

    /**
     * Returns a Store with some test arrays written to it used to test list() and exist() (can be written by the same Store implementation).
     */
    abstract Store storeWithArrays() throws ZarrException, IOException;

    @Test
    public void testInputStream() throws IOException {
        StoreHandle storeHandle = storeHandleWithData();
        InputStream is = storeHandle.getInputStream(10, 20);
        byte[] buffer = new byte[10];
        int bytesRead = is.read(buffer);
        Assertions.assertEquals(10, bytesRead);
        byte[] expectedBuffer = new byte[10];
        storeHandle.read(10, 20).get(expectedBuffer);
        Assertions.assertArrayEquals(expectedBuffer, buffer);
    }

    @Test
    public void testExists() throws ZarrException, IOException {
        Assertions.assertTrue(storeHandleWithData().exists());
        Assertions.assertFalse(storeHandleWithoutData().exists());
        Assertions.assertFalse(storeWithArrays().resolve("").exists());
    }

    @Test
    public void testListedItemsExist() throws IOException, ZarrException {
        Store store = storeWithArrays();
        if (!(store instanceof Store.ListableStore)) {
            Assertions.fail("Store is not listable");
        }
        Set<String[]> nodes = ((Store.ListableStore) store).list().limit(10).collect(Collectors.toSet());
        Assertions.assertFalse(nodes.isEmpty()); // to ensure the sensitivity of this test

        nodes.forEach(keys -> {
            System.out.println("Checking existence of key: " + String.join("/", keys));
            StoreHandle handle = store.resolve(keys);
            Assertions.assertTrue(handle.exists(), "Listed key does not exist: " + String.join("/", keys));
        });
    }

    @Test
    public abstract void testListChildren() throws ZarrException, IOException;

    @Test
    public void testGetSize() {
        StoreHandle storeHandle = storeHandleWithData();
        long size = storeHandle.getSize();
        long actual_size = storeHandle.read().remaining();
        Assertions.assertEquals(actual_size, size);
    }

    @Test
    public void testGetWithStartEnd() {
        StoreHandle storeHandle = storeHandleWithData();
        long size = storeHandle.getSize();
        System.out.println("Store size: " + size);
        if (size < 20) {
            Assertions.fail("Store size is too small to test get with start and end");
        }
        ByteBuffer buffer = storeHandle.read(5, 15);
        Assertions.assertEquals(10, buffer.remaining());

        ByteBuffer fullBuffer = storeHandle.read();
        byte[] expectedBytes = new byte[10];
        fullBuffer.position(5);
        fullBuffer.get(expectedBytes, 0, 10);
        byte[] actualBytes = new byte[10];
        buffer.get(actualBytes, 0, 10);
        Assertions.assertArrayEquals(expectedBytes, actualBytes);
    }

    @Test
    public abstract void testList() throws ZarrException, IOException;

    byte[] testData() {
        byte[] testData = new byte[1024 * 1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        return testData;
    }

    int[] testDataInt() {
        int[] testData = new int[1024 * 1024];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = i;
        }
        return testData;
    }


    Group writeTestGroupV3(StoreHandle storeHandle, boolean useParallel) throws ZarrException, IOException {

        dev.zarr.zarrjava.v3.Group group = dev.zarr.zarrjava.v3.Group.create(storeHandle);
        dev.zarr.zarrjava.v3.Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT8)
                .withChunkShape(512, 512)
        );
        array.write(ucar.ma2.Array.factory(DataType.BYTE, new int[]{1024, 1024}, testData()), useParallel);
        dev.zarr.zarrjava.v3.Group subgroup = group.createGroup("subgroup");
        dev.zarr.zarrjava.v3.Array subgrouparray = subgroup.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v3.DataType.UINT8)
                .withChunkShape(512, 512)
        );
        subgrouparray.write(ucar.ma2.Array.factory(DataType.BYTE, new int[]{1024, 1024}, testData()), useParallel);

        group.setAttributes(new Attributes(b -> b.set("some", "value")));
        return group;
    }

    void assertIsTestGroupV3(Group group, boolean useParallel) throws ZarrException, IOException {
        Stream<Node> nodes = group.list();
        List<Node> nodeList = nodes.collect(Collectors.toList());
        Assertions.assertEquals(3, nodeList.size());
        Array array = (Array) group.get("array");
        Assertions.assertNotNull(array);
        ucar.ma2.Array result = array.read(useParallel);
        Assertions.assertArrayEquals(testData(), (byte[]) result.get1DJavaArray(DataType.BYTE));
        Group subgroup = (Group) group.get("subgroup");
        Array subgrouparray = (Array) subgroup.get("array");
        result = subgrouparray.read(useParallel);
        Assertions.assertArrayEquals(testData(), (byte[]) result.get1DJavaArray(ucar.ma2.DataType.BYTE));
        Attributes attrs = group.metadata().attributes();
        Assertions.assertNotNull(attrs);
        Assertions.assertEquals("value", attrs.getString("some"));
    }


    dev.zarr.zarrjava.v2.Group writeTestGroupV2(StoreHandle storeHandle, boolean useParallel) throws ZarrException, IOException {
        dev.zarr.zarrjava.v2.Group group = dev.zarr.zarrjava.v2.Group.create(storeHandle);
        dev.zarr.zarrjava.v2.Array array = group.createArray("array", b -> b
                .withShape(1024, 1024)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT8)
                .withChunks(512, 512)
        );
        array.write(ucar.ma2.Array.factory(DataType.BYTE, new int[]{1024, 1024}, testData()), useParallel);
        group.createGroup("subgroup");
        group.setAttributes(new Attributes().set("some", "value"));
        return group;
    }

    void assertIsTestGroupV2(Group group, boolean useParallel) throws ZarrException, IOException {
        Stream<Node> nodes = group.list();
        Assertions.assertEquals(2, nodes.count());
        Array array = (Array) group.get("array");
        Assertions.assertNotNull(array);
        ucar.ma2.Array result = array.read(useParallel);
        Assertions.assertArrayEquals(testData(), (byte[]) result.get1DJavaArray(DataType.BYTE));
        Attributes attrs = group.metadata().attributes();
        Assertions.assertNotNull(attrs);
        Assertions.assertEquals("value", attrs.getString("some"));
    }

}
