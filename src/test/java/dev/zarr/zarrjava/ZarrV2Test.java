package dev.zarr.zarrjava;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

public class ZarrV2Test extends ZarrTest {
    @ParameterizedTest
    @CsvSource({"blosclz,noshuffle,0", "lz4,shuffle,6", "lz4hc,bitshuffle,3", "zlib,shuffle,5", "zstd,bitshuffle,9"})
    public void testV2createBlosc(String cname, String shuffle, int clevel) throws IOException, ZarrException {
        dev.zarr.zarrjava.v2.Array array = dev.zarr.zarrjava.v2.Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("v2_create_blosc", cname + "_" + shuffle + "_" + clevel),
            dev.zarr.zarrjava.v2.Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT8)
                .withChunks(5, 5)
                .withFillValue(1)
                .withBloscCompressor(cname, shuffle, clevel)
                .build()
        );
        array.write(new long[]{2, 2}, ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{8, 8}));

        ucar.ma2.Array outArray = array.read(new long[]{2, 2}, new int[]{8, 8});
        Assertions.assertEquals(8 * 8, outArray.getSize());
        Assertions.assertEquals(0, outArray.getByte(0));
    }

    @Test
    public void testCreate() throws IOException, ZarrException {
        dev.zarr.zarrjava.v2.DataType dataType = dev.zarr.zarrjava.v2.DataType.UINT32;

        dev.zarr.zarrjava.v2.Array array = dev.zarr.zarrjava.v2.Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("v2_create"),
            dev.zarr.zarrjava.v2.Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(dataType)
                .withChunks(5, 5)
                .withFillValue(2)
                .build()
        );
        array.write(new long[]{2, 2}, ucar.ma2.Array.factory(dataType.getMA2DataType(), new int[]{8, 8}));

        ucar.ma2.Array outArray = array.read(new long[]{2, 2}, new int[]{8, 8});
        Assertions.assertEquals(8 * 8, outArray.getSize());
        Assertions.assertEquals(0, outArray.getByte(0));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 5, 9})
    public void testCreateZlib(int level) throws IOException, ZarrException {
        dev.zarr.zarrjava.v2.Array array = dev.zarr.zarrjava.v2.Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("v2_create_zlib", String.valueOf(level)),
            dev.zarr.zarrjava.v2.Array.metadataBuilder()
                .withShape(15, 10)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT8)
                .withChunks(4, 5)
                .withFillValue(5)
                .withZlibCompressor(level)
                .build()
        );
        array.write(new long[]{2, 2}, ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{7, 6}));

        ucar.ma2.Array outArray = array.read(new long[]{2, 2}, new int[]{7, 6});
        Assertions.assertEquals(7 * 6, outArray.getSize());
        Assertions.assertEquals(0, outArray.getByte(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"BOOL", "INT8", "UINT8", "INT16", "UINT16", "INT32", "UINT32", "INT64", "UINT64", "FLOAT32", "FLOAT64"})
    public void testNoFillValue(dev.zarr.zarrjava.v2.DataType dataType) throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("v2_no_fillvalue", dataType.name());

        dev.zarr.zarrjava.v2.Array array = dev.zarr.zarrjava.v2.Array.create(
            storeHandle,
            dev.zarr.zarrjava.v2.Array.metadataBuilder()
                .withShape(15, 10)
                .withDataType(dataType)
                .withChunks(4, 5)
                .build()
        );
        Assertions.assertNull(array.metadata.fillValue);

        ucar.ma2.Array outArray = array.read(new long[]{0, 0}, new int[]{1, 1});
        if (dataType == dev.zarr.zarrjava.v2.DataType.BOOL) {
            Assertions.assertFalse(outArray.getBoolean(0));
        } else {
            Assertions.assertEquals(0, outArray.getByte(0));
        }

        dev.zarr.zarrjava.v2.Array array2 = dev.zarr.zarrjava.v2.Array.open(
            storeHandle
        );
        Assertions.assertNull(array2.metadata.fillValue);
    }

    @Test
    public void testGroup() throws IOException, ZarrException {
        FilesystemStore fsStore = new FilesystemStore(TESTOUTPUT);

        dev.zarr.zarrjava.v2.Group group = dev.zarr.zarrjava.v2.Group.create(fsStore.resolve("v2_testgroup"));
        dev.zarr.zarrjava.v2.Group group2 = group.createGroup("test2");
        dev.zarr.zarrjava.v2.Array array = group2.createArray("array", b ->
            b.withShape(10, 10)
                .withDataType(dev.zarr.zarrjava.v2.DataType.UINT8)
                .withChunks(5, 5)
        );
        array.write(new long[]{2, 2}, ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{8, 8}));

        Assertions.assertArrayEquals(((dev.zarr.zarrjava.v2.Array) ((dev.zarr.zarrjava.v2.Group) group.listAsArray()[0]).listAsArray()[0]).metadata.chunks, new int[]{5, 5});
    }

}
