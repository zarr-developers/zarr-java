package dev.zarr.zarrjava;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.Array;
import dev.zarr.zarrjava.v2.ArrayMetadata;
import dev.zarr.zarrjava.v2.DataType;
import dev.zarr.zarrjava.v2.Group;
import dev.zarr.zarrjava.v2.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ZarrV2Test extends ZarrTest {
    @ParameterizedTest
    @CsvSource({"blosclz,noshuffle,0", "lz4,shuffle,6", "lz4hc,bitshuffle,3", "zlib,shuffle,5", "zstd,bitshuffle,9"})
    public void testV2createBlosc(String cname, String shuffle, int clevel) throws IOException, ZarrException {
        Array array = Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("v2_create_blosc", cname + "_" + shuffle + "_" + clevel),
            Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
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
        DataType dataType = DataType.UINT32;

        Array array = Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("v2_create"),
            Array.metadataBuilder()
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
        Array array = Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("v2_create_zlib", String.valueOf(level)),
            Array.metadataBuilder()
                .withShape(15, 10)
                .withDataType(DataType.UINT8)
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
    public void testNoFillValue(DataType dataType) throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("v2_no_fillvalue", dataType.name());

        Array array = Array.create(
            storeHandle,
            Array.metadataBuilder()
                .withShape(15, 10)
                .withDataType(dataType)
                .withChunks(4, 5)
                .build()
        );
        Assertions.assertNull(array.metadata.fillValue);

        ucar.ma2.Array outArray = array.read(new long[]{0, 0}, new int[]{1, 1});
        if (dataType == DataType.BOOL) {
            Assertions.assertFalse(outArray.getBoolean(0));
        } else {
            Assertions.assertEquals(0, outArray.getByte(0));
        }

        Array array2 = Array.open(
            storeHandle
        );
        Assertions.assertNull(array2.metadata.fillValue);
    }


    @Test
    public void testOpen() throws ZarrException, IOException {
        StoreHandle arrayHandle = new FilesystemStore(TESTDATA).resolve("v2_sample", "subgroup", "array");
        StoreHandle groupHandle = new FilesystemStore(TESTDATA).resolve("v2_sample");
        StoreHandle v3Handle = new FilesystemStore(TESTDATA).resolve("l4_sample");

        Array array = (Array) Node.open(arrayHandle);
        Assertions.assertEquals(3, (array).metadata.shape.length);

        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayHandle);
        Assertions.assertEquals(3, (array).metadata.shape.length);

        array = (Array) dev.zarr.zarrjava.core.Node.open(arrayHandle);
        Assertions.assertEquals(3, (array).metadata.shape.length);

        Group group = (Group) Node.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("subgroup"));

        group = (Group) dev.zarr.zarrjava.core.Group.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("subgroup"));

        group = (Group) dev.zarr.zarrjava.core.Node.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("subgroup"));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(TESTDATA.resolve("non_existing")));
        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(v3Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> Group.open(v3Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> Array.open(v3Handle));
    }

    @Test
    public void testOpenOverloads() throws ZarrException, IOException {
        Path arrayPath = TESTDATA.resolve("v2_sample").resolve("subgroup").resolve("array");
        Path groupPath = TESTDATA.resolve("v2_sample");
        Path v3GroupPath = TESTDATA.resolve("l4_sample");

        Array array = (Array) Node.open(arrayPath);
        Assertions.assertEquals(3, (array).metadata.shape.length);
        array = (Array) Node.open(arrayPath.toString());
        Assertions.assertEquals(3, (array).metadata.shape.length);

        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayPath);
        Assertions.assertEquals(3, (array).metadata.shape.length);
        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayPath.toString());
        Assertions.assertEquals(3, (array).metadata.shape.length);

        array = (Array) dev.zarr.zarrjava.core.Node.open(arrayPath);
        Assertions.assertEquals(3, (array).metadata.shape.length);
        array = (Array) dev.zarr.zarrjava.core.Node.open(arrayPath.toString());
        Assertions.assertEquals(3, (array).metadata.shape.length);

        Group group = (Group) Node.open(groupPath);
        Assertions.assertInstanceOf(Group.class, group.get("subgroup"));
        group = (Group) Node.open(groupPath.toString());
        Assertions.assertInstanceOf(Group.class, group.get("subgroup"));

        group = (Group) dev.zarr.zarrjava.core.Group.open(groupPath);
        Assertions.assertInstanceOf(Group.class, group.get("subgroup"));
        group = (Group) dev.zarr.zarrjava.core.Group.open(groupPath.toString());
        Assertions.assertInstanceOf(Group.class, group.get("subgroup"));

        group = (Group) dev.zarr.zarrjava.core.Node.open(groupPath);
        Assertions.assertInstanceOf(Group.class, group.get("subgroup"));
        group = (Group) dev.zarr.zarrjava.core.Node.open(groupPath.toString());
        Assertions.assertInstanceOf(Group.class, group.get("subgroup"));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(TESTDATA.resolve("non_existing")));
        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(TESTDATA.resolve("non_existing").toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(v3GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(v3GroupPath.toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> Group.open(v3GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> Group.open(v3GroupPath.toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> Array.open(v3GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> Array.open(v3GroupPath.toString()));
    }

    @Test
    public void testCreateArray() throws ZarrException, IOException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testCreateArrayV2");
        Path storeHandlePath = TESTOUTPUT.resolve("testCreateArrayV2Path");
        String storeHandleString = String.valueOf(TESTOUTPUT.resolve("testCreateArrayV2String"));
        ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunks(5, 5)
                .build();

        Array.create(storeHandle, arrayMetadata);
        Assertions.assertTrue(storeHandle.resolve(".zarray").exists());

        Array.create(storeHandlePath, arrayMetadata);
        Assertions.assertTrue(Files.exists(storeHandlePath.resolve(".zarray")));

        Array.create(storeHandleString, arrayMetadata);
        Assertions.assertTrue(Files.exists(Paths.get(storeHandleString).resolve(".zarray")));
    }

    @Test
    public void testGroup() throws IOException, ZarrException {
        FilesystemStore fsStore = new FilesystemStore(TESTOUTPUT);

        Group group = Group.create(fsStore.resolve("v2_testgroup"));
        Group group2 = group.createGroup("test2");
        Array array = group2.createArray("array", b ->
            b.withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunks(5, 5)
        );
        array.write(new long[]{2, 2}, ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{8, 8}));

        Assertions.assertArrayEquals(new int[]{5, 5}, ((Array) ((Group) group.listAsArray()[0]).listAsArray()[0]).metadata.chunks);
    }


    @Test
    public void testCreateGroup() throws ZarrException, IOException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testCreateGroupV2");
        Path storeHandlePath = TESTOUTPUT.resolve("testCreateGroupV2Path");
        String storeHandleString = String.valueOf(TESTOUTPUT.resolve("testCreateGroupV2String"));

        Group.create(storeHandle);
        Assertions.assertTrue(storeHandle.resolve(".zgroup").exists());

        Group.create(storeHandlePath);
        Assertions.assertTrue(Files.exists(storeHandlePath.resolve(".zgroup")));

        Group.create(storeHandleString);
        Assertions.assertTrue(Files.exists(Paths.get(storeHandleString).resolve(".zgroup")));
    }
}