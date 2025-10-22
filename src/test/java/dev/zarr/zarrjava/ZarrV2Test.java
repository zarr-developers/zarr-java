package dev.zarr.zarrjava;

import dev.zarr.zarrjava.core.Attributes;
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
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ZarrV2Test extends ZarrTest {
    @ParameterizedTest
    @CsvSource({"blosclz,noshuffle,0", "lz4,shuffle,6", "lz4hc,bitshuffle,3", "zlib,shuffle,5", "zstd,bitshuffle,9"})
    public void testCreateBlosc(String cname, String shuffle, int clevel) throws IOException, ZarrException {
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



    @ParameterizedTest
    @CsvSource({
        "BOOL", "FLOAT64"
    })
    public void testReadBloscDetectTypesize(DataType dt) throws IOException, ZarrException {
        String arrayname = dt == DataType.BOOL ? "bool" : "double";
        StoreHandle storeHandle = new FilesystemStore(TESTDATA).resolve("v2_sample", arrayname);
        Array array = Array.open(storeHandle);
        ucar.ma2.Array output = array.read(new long[]{0, 0, 0}, new int[]{3, 4, 5});
        Assertions.assertEquals(dt, array.metadata().dataType);
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
        Assertions.assertNull(array.metadata().fillValue);

        ucar.ma2.Array outArray = array.read(new long[]{0, 0}, new int[]{1, 1});
        if (dataType == DataType.BOOL) {
            Assertions.assertFalse(outArray.getBoolean(0));
        } else {
            Assertions.assertEquals(0, outArray.getByte(0));
        }

        Array array2 = Array.open(
            storeHandle
        );
        Assertions.assertNull(array2.metadata().fillValue);
    }


    @Test
    public void testOpen() throws ZarrException, IOException {
        StoreHandle arrayHandle = new FilesystemStore(TESTDATA).resolve("v2_sample", "subgroup", "array");
        StoreHandle groupHandle = new FilesystemStore(TESTDATA).resolve("v2_sample");
        StoreHandle v3Handle = new FilesystemStore(TESTDATA).resolve("l4_sample");

        Array array = (Array) Node.open(arrayHandle);
        Assertions.assertEquals(3, array.metadata().shape.length);

        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayHandle);
        Assertions.assertEquals(3, array.metadata().shape.length);

        array = (Array) dev.zarr.zarrjava.core.Node.open(arrayHandle);
        Assertions.assertEquals(3, array.metadata().shape.length);

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
        Assertions.assertEquals(3, array.metadata().shape.length);
        array = (Array) Node.open(arrayPath.toString());
        Assertions.assertEquals(3, array.metadata().shape.length);

        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayPath);
        Assertions.assertEquals(3, array.metadata().shape.length);
        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayPath.toString());
        Assertions.assertEquals(3, array.metadata().shape.length);

        array = (Array) dev.zarr.zarrjava.core.Node.open(arrayPath);
        Assertions.assertEquals(3, array.metadata().shape.length);
        array = (Array) dev.zarr.zarrjava.core.Node.open(arrayPath.toString());
        Assertions.assertEquals(3, array.metadata().shape.length);

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

        Assertions.assertArrayEquals(new int[]{5, 5}, ((Array) ((Group) group.listAsArray()[0]).listAsArray()[0]).metadata().chunks);
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

    static void assertListEquals(List<Object> a, List<Object> b) {
        Assertions.assertEquals(a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            Object aval = a.get(i);
            Object bval = b.get(i);
            if (aval instanceof List && bval instanceof List) {
                assertListEquals((List<Object>) aval, (List<Object>) bval);
            } else {
                Assertions.assertEquals(aval, bval);
            }
        }
    }

    @Test
    public void testAttributes() throws IOException, ZarrException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testAttributesV2");
        ArrayList<Object> list = new ArrayList<Object>(){
            {
                add(1);
                add(2.0d);
                add("string");
            }
        };
        int[] intArray = new int[]{1,2,3};
        long[] longArray = new long[]{1,2,3};
        double[] doubleArray = new double[]{1.0,2.0,3.0};
        float[] floatArray = new float[]{1.0f,2.0f,3.0f};

        ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunks(5, 5)
                .withAttributes(new Attributes() {{
                    put("string", "stringvalue");
                    put("int", 42);
                    put("float", 0.5f);
                    put("double", 3.14);
                    put("boolean", true);
                    put("list", list);
                    put("int_array", intArray);
                    put("long_array", longArray);
                    put("double_array", doubleArray);
                    put("float_array", floatArray);
                    put("nested", new Attributes() {{
                        put("element", "value");
                    }});
                    put("array_of_attributes", new Attributes[] {
                        new Attributes() {{ put("a", 1); }},
                        new Attributes() {{ put("b", 2); }}
                    });
                }})
                .withAttributes(new Attributes() {{
                    put("another", "attribute");
                }})
                .build();

        Array array = Array.create(storeHandle, arrayMetadata);
        Assertions.assertEquals("stringvalue", array.metadata().attributes().getString("string"));
        Assertions.assertEquals(42, array.metadata().attributes().getInt("int"));
        Assertions.assertEquals(0.5, array.metadata().attributes().getFloat("float"));
        Assertions.assertEquals(3.14, array.metadata().attributes().getDouble("double"));
        Assertions.assertTrue(array.metadata().attributes().getBoolean("boolean"));
        assertListEquals(list, array.metadata().attributes().getList("list"));
        Assertions.assertArrayEquals(intArray, array.metadata().attributes().getIntArray("int_array"));
        Assertions.assertArrayEquals(longArray, array.metadata().attributes().getLongArray("long_array"));
        Assertions.assertArrayEquals(doubleArray, array.metadata().attributes().getDoubleArray("double_array"));
        Assertions.assertArrayEquals(floatArray, array.metadata().attributes().getFloatArray("float_array"));
        Assertions.assertEquals("value", array.metadata().attributes().getAttributes("nested").getString("element"));
        Assertions.assertArrayEquals(
            new Attributes[]{
                new Attributes() {{ put("a", 1); }},
                new Attributes() {{ put("b", 2); }}
            },
            array.metadata().attributes().getArray("array_of_attributes", Attributes.class)
        );
        Assertions.assertEquals("attribute", array.metadata().attributes().getString("another"));

        Array arrayOpened = Array.open(storeHandle);
        Assertions.assertEquals("stringvalue", arrayOpened.metadata().attributes().getString("string"));
        Assertions.assertEquals(42, arrayOpened.metadata().attributes().getInt("int"));
        Assertions.assertEquals(0.5, arrayOpened.metadata().attributes().getFloat("float"));
        Assertions.assertEquals(3.14, arrayOpened.metadata().attributes().getDouble("double"));
        Assertions.assertTrue(arrayOpened.metadata().attributes().getBoolean("boolean"));
        assertListEquals(list, arrayOpened.metadata().attributes().getList("list"));
        Assertions.assertArrayEquals(intArray, arrayOpened.metadata().attributes().getIntArray("int_array"));
        Assertions.assertArrayEquals(longArray, arrayOpened.metadata().attributes().getLongArray("long_array"));
        Assertions.assertArrayEquals(doubleArray, arrayOpened.metadata().attributes().getDoubleArray("double_array"));
        Assertions.assertArrayEquals(floatArray, arrayOpened.metadata().attributes().getFloatArray("float_array"));
        Assertions.assertEquals("value", arrayOpened.metadata().attributes().getAttributes("nested").getString("element"));
        Assertions.assertArrayEquals(
            new Attributes[]{
                new Attributes() {{ put("a", 1); }},
                new Attributes() {{ put("b", 2); }}
            },
            arrayOpened.metadata().attributes().getArray("array_of_attributes", Attributes.class)
        );
        Assertions.assertEquals("attribute", arrayOpened.metadata().attributes().getString("another"));
    }
}