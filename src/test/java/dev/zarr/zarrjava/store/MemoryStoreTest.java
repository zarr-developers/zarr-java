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

    @ParameterizedTest
    @CsvSource({"false", "true",})
    public void testMemoryStoreV3(boolean useParallel) throws ZarrException, IOException {
        int[] testData = testDataInt();

        dev.zarr.zarrjava.v3.Group group = dev.zarr.zarrjava.v3.Group.create(new MemoryStore().resolve());
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

        dev.zarr.zarrjava.v2.Group group = dev.zarr.zarrjava.v2.Group.create(new MemoryStore().resolve());
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
