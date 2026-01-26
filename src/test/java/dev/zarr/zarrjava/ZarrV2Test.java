package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.MemoryStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dev.zarr.zarrjava.core.Node.ZARRAY;

public class ZarrV2Test extends ZarrTest {
    static Stream<Function<ArrayMetadataBuilder, ArrayMetadataBuilder>> compressorBuilder() {
        return Stream.of(
                ArrayMetadataBuilder::withBloscCompressor,
                ArrayMetadataBuilder::withZlibCompressor,
                b -> b
        );
    }

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
        array.read(new long[]{0, 0, 0}, new int[]{3, 4, 5});
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

        Group.create(storeHandleString, new Attributes(b -> b.set("some", "value")));
        Assertions.assertTrue(Files.exists(Paths.get(storeHandleString).resolve(".zgroup")));
        Assertions.assertTrue(Files.exists(Paths.get(storeHandleString).resolve(".zattrs")));
    }

    @Test
    public void testAttributes() throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testAttributesV2");

        ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunks(5, 5)
                .putAttribute("specific", "attribute")
                .withAttributes(defaultTestAttributes())
                .withAttributes(new Attributes() {{
                    put("another", "attribute");
                }})
                .build();

        Array array = Array.create(storeHandle, arrayMetadata);
        assertContainsTestAttributes(array.metadata().attributes());
        Assertions.assertEquals("attribute", array.metadata().attributes().getString("specific"));
        Assertions.assertEquals("attribute", array.metadata().attributes().getString("another"));

        Array arrayOpened = Array.open(storeHandle);
        assertContainsTestAttributes(array.metadata().attributes());
        Assertions.assertEquals("attribute", arrayOpened.metadata().attributes().getString("another"));
        Assertions.assertEquals("attribute", arrayOpened.metadata().attributes().getString("specific"));
    }

    @Test
    public void testSetAndUpdateAttributes() throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testSetAttributesV2");

        ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunks(5, 5)
                .withAttributes(new Attributes(b -> b.set("some", "value")))
                .build();

        Array array = Array.create(storeHandle, arrayMetadata);
        Assertions.assertEquals("value", array.metadata().attributes().getString("some"));
        array.setAttributes(defaultTestAttributes());
        array = Array.open(storeHandle);
        assertContainsTestAttributes(array.metadata().attributes());
        Assertions.assertNull(array.metadata().attributes().get("some"));

        // add attribute
        array = array.updateAttributes(b -> b.set("new_attribute", "new_value"));
        Assertions.assertEquals("new_value", array.metadata().attributes().getString("new_attribute"));
        array = Array.open(storeHandle);
        Assertions.assertEquals("new_value", array.metadata().attributes().getString("new_attribute"));

        // delete attribute
        array = array.updateAttributes(b -> b.delete("new_value"));
        Assertions.assertNull(array.metadata().attributes().get("new_value"));
        array = Array.open(storeHandle);
        Assertions.assertNull(array.metadata().attributes().get("new_value"));

        assertContainsTestAttributes(array.metadata().attributes());
    }

    @Test
    public void testResizeArray() throws IOException, ZarrException {
        int[] testData = new int[10 * 10];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testResizeArrayV2");
        ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT32)
                .withChunks(5, 5)
                .withFillValue(1)
                .build();
        ucar.ma2.DataType ma2DataType = arrayMetadata.dataType.getMA2DataType();
        Array array = Array.create(storeHandle, arrayMetadata);
        array.write(new long[]{0, 0}, ucar.ma2.Array.factory(ma2DataType, new int[]{10, 10}, testData));

        array = array.resize(new long[]{20, 15});
        Assertions.assertArrayEquals(new int[]{20, 15}, array.read().getShape());

        ucar.ma2.Array data = array.read(new long[]{0, 0}, new int[]{10, 10});
        Assertions.assertArrayEquals(testData, (int[]) data.get1DJavaArray(ma2DataType));

        data = array.read(new long[]{10, 10}, new int[]{5, 5});
        int[] expectedData = new int[5 * 5];
        Arrays.fill(expectedData, 1);
        Assertions.assertArrayEquals(expectedData, (int[]) data.get1DJavaArray(ma2DataType));
    }

    @Test
    public void testGroupAttributes() throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testGroupAttributesV2");

        Group group = Group.create(storeHandle, new Attributes() {{
            put("group_attr", "group_value");
        }});

        Assertions.assertEquals("group_value", group.metadata().attributes().getString("group_attr"));

        group = Group.open(storeHandle);
        Assertions.assertEquals("group_value", group.metadata().attributes().getString("group_attr"));
    }

    @ParameterizedTest
    @MethodSource("compressorBuilder")
    public void testZarrJsonFormat(Function<ArrayMetadataBuilder, ArrayMetadataBuilder> compressorBuilder) throws ZarrException, IOException {
        // regression test: ensure that 'id' keyword of codecs is only written once.
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testZarrJsonFormatV2").resolve(String.valueOf(compressorBuilder.hashCode()));
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunks(6, 6);
        builder = compressorBuilder.apply(builder);
        Array.create(storeHandle, builder.build());

        try (BufferedReader reader = Files.newBufferedReader(storeHandle.resolve(ZARRAY).toPath())) {
            String jsonInString = reader.lines().collect(Collectors.joining(System.lineSeparator()));
            JsonNode JSON = new ObjectMapper().readTree(jsonInString);
            Assertions.assertEquals(JSON.toPrettyString(), jsonInString);
        }
    }

    @Test
    public void testMemoryStore() throws ZarrException, IOException {
        StoreHandle storeHandle = new MemoryStore().resolve();
        Group group = Group.create(storeHandle);
        Array array = group.createArray("array", b -> b
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunks(5, 5)
        );
        group.createGroup("subgroup");
        Assertions.assertEquals(2, group.list().count());
        for (String s : storeHandle.list().toArray(String[]::new))
            System.out.println(s);
    }

    @Test
    public void testDefaultChunkShape() throws IOException, ZarrException {
        // Test with a small array (< 512 elements per dimension)
        Array smallArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("v2_default_chunks_small"),
                Array.metadataBuilder()
                        .withShape(100, 50)
                        .withDataType(DataType.UINT8)
                        .build()
        );
        Assertions.assertEquals(2, smallArray.metadata().chunks.length);
        // Both dimensions < 512, so chunks should equal shape
        Assertions.assertEquals(100, smallArray.metadata().chunks[0]);
        Assertions.assertEquals(50, smallArray.metadata().chunks[1]);

        // Test with a larger array (> 512 elements per dimension)
        Array largeArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("v2_default_chunks_large"),
                Array.metadataBuilder()
                        .withShape(2000, 1500)
                        .withDataType(DataType.UINT8)
                        .build()
        );
        Assertions.assertEquals(2, largeArray.metadata().chunks.length);
        // Chunks should be calculated based on division by 512
        Assertions.assertTrue(largeArray.metadata().chunks[0] > 0);
        Assertions.assertTrue(largeArray.metadata().chunks[0] < 2000);
        Assertions.assertTrue(largeArray.metadata().chunks[1] > 0);
        Assertions.assertTrue(largeArray.metadata().chunks[1] < 1500);

        // Test with mixed dimensions
        Array mixedArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("v2_default_chunks_mixed"),
                Array.metadataBuilder()
                        .withShape(1024, 100, 2048)
                        .withDataType(DataType.UINT8)
                        .build()
        );
        Assertions.assertEquals(3, mixedArray.metadata().chunks.length);
        // Verify chunks are reasonable
        Assertions.assertTrue(mixedArray.metadata().chunks[0] > 0);
        Assertions.assertTrue(mixedArray.metadata().chunks[0] <= 1024);
        Assertions.assertEquals(100, mixedArray.metadata().chunks[1]);  // < 512, should equal shape
        Assertions.assertTrue(mixedArray.metadata().chunks[2] > 0);
        Assertions.assertTrue(mixedArray.metadata().chunks[2] <= 2048);
    }
}
