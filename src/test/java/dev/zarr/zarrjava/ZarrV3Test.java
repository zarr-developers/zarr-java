package dev.zarr.zarrjava;

import dev.zarr.zarrjava.core.Attributes;

import com.fasterxml.jackson.databind.JsonMappingException;
import dev.zarr.zarrjava.store.*;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.v3.Node;
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import dev.zarr.zarrjava.v3.codec.core.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import ucar.ma2.MAMath;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static dev.zarr.zarrjava.core.ArrayMetadata.parseFillValue;
import static org.junit.Assert.assertThrows;

public class ZarrV3Test extends ZarrTest {

    static Stream<Function<CodecBuilder, CodecBuilder>> invalidCodecBuilder() {
        return Stream.of(
            c -> c.withBytes(BytesCodec.Endian.LITTLE).withBytes(BytesCodec.Endian.LITTLE),
            c -> c.withBlosc().withBytes(BytesCodec.Endian.LITTLE),
            c -> c.withBytes(BytesCodec.Endian.LITTLE).withTranspose(new int[]{1, 0}),
            c -> c.withTranspose(new int[]{1, 0}).withBytes(BytesCodec.Endian.LITTLE).withTranspose(new int[]{1, 0})
        );
    }

    @ParameterizedTest
    @MethodSource("invalidCodecBuilder")
    public void testCheckInvalidCodecConfiguration(Function<CodecBuilder, CodecBuilder> codecBuilder) {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("invalid_codec_config", String.valueOf(codecBuilder.hashCode()));
        ArrayMetadataBuilder builder = Array.metadataBuilder()
            .withShape(4, 4)
            .withDataType(DataType.UINT32)
            .withChunkShape(2, 2)
            .withCodecs(codecBuilder);

        assertThrows(ZarrException.class, () -> Array.create(storeHandle, builder.build()));
    }

    @Test
    public void testLargerChunkSizeThanArraySize() throws ZarrException, IOException {
        int[] testData = new int[16 * 16 * 16];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("larger_chunk_size_than_array");
        ArrayMetadata metadata = Array.metadataBuilder()
            .withShape(16, 16, 16)
            .withDataType(DataType.UINT32)
            .withChunkShape(32, 32, 32)
            .withFillValue(0)
            .build();
        Array writeArray = Array.create(storeHandle, metadata);
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{16, 16, 16}, testData));

        //read in zarr-java
        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
    }

    static Stream<int[]> invalidChunkSizes() {
        return Stream.of(
            new int[]{1},
            new int[]{1, 1, 1}
        );
    }

    @ParameterizedTest
    @MethodSource("invalidChunkSizes")
    public void testCheckInvalidChunkDimensions(int[] chunkSize) {
        long[] shape = new long[]{4, 4};

        ArrayMetadataBuilder builder = Array.metadataBuilder()
            .withShape(shape)
            .withDataType(DataType.UINT32)
            .withChunkShape(chunkSize);

        assertThrows(ZarrException.class, builder::build);
    }

    static Stream<int[]> invalidShardSizes() {
        return Stream.of(
            new int[]{4},           //wrong dims
            new int[]{4, 4, 4},     //wrong dims
            new int[]{1, 1},        //smaller than inner chunk shape
            new int[]{5, 5},        //no exact multiple of inner chunk shape
            new int[]{2, 1},        //smaller than inner chunk shape in 2nd dimension
            new int[]{2, 5}         //no exact multiple of inner chunk shape in 2nd dimension
        );
    }
    static Stream<Arguments> invalidShardSizesWithNested() {
        return invalidShardSizes().flatMap(shardSize ->
            Stream.of(true, false).map(nested -> Arguments.of(shardSize, nested))
        );
    }
    @ParameterizedTest
    @MethodSource("invalidShardSizesWithNested")
    public void testCheckShardingBounds(int[] shardSize, boolean nested) {
        long[] shape = new long[]{10, 10};
        int[] innerChunkSize = new int[]{2, 2};

        ArrayMetadataBuilder builder = Array.metadataBuilder()
            .withShape(shape)
            .withDataType(DataType.UINT32).withChunkShape(shardSize);

        if (nested) {
            int[] nestedChunkSize = new int[]{4, 4};
            builder = builder.withCodecs(c -> c.withSharding(new int[]{2, 2}, c1 -> c1.withSharding(nestedChunkSize, c2 -> c2.withBytes("LITTLE"))));
        }
        builder = builder.withCodecs(c -> c.withSharding(innerChunkSize, c1 -> c1.withBytes("LITTLE")));
        assertThrows(ZarrException.class, builder::build);
    }

    @ParameterizedTest
    @CsvSource({"0,true", "0,false", "5, true", "5, false"})
    public void testZstdCodecReadWrite(int clevel, boolean checksum) throws ZarrException, IOException {
        int[] testData = new int[16 * 16 * 16];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testZstdCodecReadWrite", "checksum_" + checksum, "clevel_" + clevel);
        ArrayMetadataBuilder builder = Array.metadataBuilder()
            .withShape(16, 16, 16)
            .withDataType(DataType.UINT32)
            .withChunkShape(2, 4, 8)
            .withFillValue(0)
            .withCodecs(c -> c.withZstd(clevel, checksum));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{16, 16, 16}, testData));

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
    }

    @Test
    public void testTransposeCodec() throws ZarrException {
        ucar.ma2.Array testData = ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{2, 3, 3}, new int[]{
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17});
        ucar.ma2.Array testDataTransposed120 = ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{3, 3, 2}, new int[]{
            0, 9, 1, 10, 2, 11, 3, 12, 4, 13, 5, 14, 6, 15, 7, 16, 8, 17});

        TransposeCodec transposeCodec = new TransposeCodec(new TransposeCodec.Configuration(new int[]{1, 2, 0}));
        transposeCodec.setCoreArrayMetadata(new ArrayMetadata.CoreArrayMetadata(
            new long[]{2, 3, 3},
            new int[]{2, 3, 3},
            DataType.UINT32,
            null));

        assert MAMath.equals(testDataTransposed120, transposeCodec.encode(testData));
        assert MAMath.equals(testData, transposeCodec.decode(testDataTransposed120));
    }

    static Stream<int[]> invalidTransposeOrder() {
        return Stream.of(
            new int[]{1, 0, 0},
            new int[]{1, 2, 3},
            new int[]{1, 2, 3, 0},
            new int[]{1, 2}
        );
    }

    @ParameterizedTest
    @MethodSource("invalidTransposeOrder")
    public void testCheckInvalidTransposeOrder(int[] transposeOrder) throws Exception {
        int[] shapeInt = new int[]{2, 3, 3};
        long[] shapeLong = new long[]{2, 3, 3};

        TransposeCodec transposeCodec = new TransposeCodec(new TransposeCodec.Configuration(transposeOrder));
        transposeCodec.setCoreArrayMetadata(new ArrayMetadata.CoreArrayMetadata(
            shapeLong,
            shapeInt,
            DataType.UINT32,
            null));

        ucar.ma2.Array testData = ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, shapeInt);
        assertThrows(ZarrException.class, () -> transposeCodec.encode(testData));
    }

    @Test
    public void testShardingReadCutout() throws IOException, ZarrException {
        Array array = Array.open(new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1"));

        ucar.ma2.Array outArray = array.read(new long[]{0, 3073, 3073, 513}, new int[]{1, 64, 64, 64});
        Assertions.assertEquals(64 * 64 * 64, outArray.getSize());
        Assertions.assertEquals(-98, outArray.getByte(0));
    }

    @Test
    public void testAccess() throws IOException, ZarrException {
        Array readArray = Array.open(new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1"));

        ucar.ma2.Array outArray = readArray.access().withOffset(0, 3073, 3073, 513)
            .withShape(1, 64, 64, 64)
            .read();
        Assertions.assertEquals(64 * 64 * 64, outArray.getSize());
        Assertions.assertEquals(-98, outArray.getByte(0));

        Array writeArray = Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("l4_sample_2", "color", "1"),
            readArray.metadata()
        );
        writeArray.access().withOffset(0, 3073, 3073, 513).write(outArray);
    }

    @ParameterizedTest
    @ValueSource(strings = {"start", "end"})
    public void testShardingReadWrite(String indexLocation) throws IOException, ZarrException {
        Array readArray = Array.open(
            new FilesystemStore(TESTDATA).resolve("sharding_index_location", indexLocation));
        ucar.ma2.Array readArrayContent = readArray.read();
        Array writeArray = Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("sharding_index_location", indexLocation),
            readArray.metadata()
        );
        writeArray.write(readArrayContent);
        ucar.ma2.Array outArray = writeArray.read();

        assert MultiArrayUtils.allValuesEqual(readArrayContent, outArray);
    }

    @Test
    public void testCodecs() throws IOException, ZarrException {
        int[] readShape = new int[]{1, 1, 1024, 1024};
        Array readArray = Array.open(
            new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "8-8-2"));
        ucar.ma2.Array readArrayContent = readArray.read(new long[4], readShape);
        {
            Array gzipArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("l4_sample_gzip", "color", "8-8-2"),
                Array.metadataBuilder(readArray.metadata()).withCodecs(c -> c.withGzip(5)).build()
            );
            gzipArray.write(readArrayContent);
            ucar.ma2.Array outGzipArray = gzipArray.read(new long[4], readShape);
            assert MultiArrayUtils.allValuesEqual(outGzipArray, readArrayContent);
        }
        {
            Array bloscArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("l4_sample_blosc", "color", "8-8-2"),
                Array.metadataBuilder(readArray.metadata()).withCodecs(c -> c.withBlosc("zstd", 5)).build()
            );
            bloscArray.write(readArrayContent);
            ucar.ma2.Array outBloscArray = bloscArray.read(new long[4], readShape);
            assert MultiArrayUtils.allValuesEqual(outBloscArray, readArrayContent);
        }
        {
            Array zstdArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("l4_sample_zstd", "color", "8-8-2"),
                Array.metadataBuilder(readArray.metadata()).withCodecs(c -> c.withZstd(10)).build()
            );
            zstdArray.write(readArrayContent);
            ucar.ma2.Array outZstdArray = zstdArray.read(new long[4], readShape);
            assert MultiArrayUtils.allValuesEqual(outZstdArray, readArrayContent);
        }
    }

    @Test
    public void testArrayMetadataBuilder() throws ZarrException {
        long[] shape = new long[]{1, 4096, 4096, 1536};
        DataType dataType = DataType.UINT32;
        int[] chunkShape = new int[]{1, 1024, 1024, 1024};
        int fillValue = 0;

        ArrayMetadata metadata = Array.metadataBuilder()
            .withShape(shape)
            .withDataType(dataType)
            .withChunkShape(chunkShape)
            .withFillValue(fillValue)
            .withCodecs(
                c -> c.withSharding(new int[]{1, 32, 32, 32}, CodecBuilder::withBlosc))
            .build();
        Assertions.assertArrayEquals(shape, metadata.shape);
        Assertions.assertEquals(dataType, metadata.dataType);
        Assertions.assertArrayEquals(chunkShape, metadata.chunkShape());
        Assertions.assertEquals(fillValue, metadata.fillValue);
        Assertions.assertEquals(1, metadata.codecs.length);
        ShardingIndexedCodec shardingCodec = (ShardingIndexedCodec) metadata.codecs[0];
        Assertions.assertInstanceOf(ShardingIndexedCodec.class, shardingCodec);
        Assertions.assertInstanceOf(BytesCodec.class, shardingCodec.configuration.codecs[0]);
        Assertions.assertInstanceOf(BloscCodec.class, shardingCodec.configuration.codecs[1]);
    }

    @Test
    public void testFillValue() throws ZarrException {
        Assertions.assertEquals(0, (int) parseFillValue(0, DataType.UINT32));
        Assertions.assertEquals(50462976, (int) parseFillValue("0x00010203", DataType.UINT32));
        Assertions.assertEquals(2, (byte) parseFillValue("0b00000010", DataType.UINT8));
        assert Double.isNaN((double) parseFillValue("NaN", DataType.FLOAT64));
        assert Double.isInfinite((double) parseFillValue("-Infinity", DataType.FLOAT64));
    }

    @Test
    public void testReadme1() throws IOException, ZarrException {
        Group hierarchy = Group.open(
            new HttpStore("https://static.webknossos.org/data/zarr_v3")
                .resolve("l4_sample")
        );
        Group color = (Group) hierarchy.get("color");
        Array array = (Array) color.get("1");
        ucar.ma2.Array outArray = array.read(
            new long[]{0, 3073, 3073, 513}, // offset
            new int[]{1, 64, 64, 64} // shape
        );
        Assertions.assertEquals(64 * 64 * 64, outArray.getSize());
    }

    @Test
    public void testReadme2() throws IOException, ZarrException {
        Array array = Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("testoutput", "color", "1"),
            Array.metadataBuilder()
                .withShape(1, 4096, 4096, 1536)
                .withDataType(DataType.UINT32)
                .withChunkShape(1, 1024, 1024, 1024)
                .withFillValue(0)
                .withCodecs(c -> c.withSharding(new int[]{1, 32, 32, 32}, c1 -> c1.withBlosc()))
                .build()
        );
        ucar.ma2.Array data = ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{1, 1, 2, 2}, new int[]{1, 2, 3, 4});
        array.write(
            new long[]{0, 0, 0, 0}, // offset
            data
        );
        ucar.ma2.Array output = array.read(new long[]{0, 0, 0, 0}, new int[]{1, 1, 2, 2});
        assert MultiArrayUtils.allValuesEqual(data, output);
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "2-2-1", "4-4-1", "16-16-4"})
    public void testReadL4Sample(String mag) throws IOException, ZarrException {
        StoreHandle httpStoreHandle = new HttpStore("https://static.webknossos.org/data/zarr_v3/").resolve("l4_sample", "color", mag);
        StoreHandle localStoreHandle = new FilesystemStore(TESTDATA).resolve("l4_sample", "color", mag);

        Array httpArray = Array.open(httpStoreHandle);
        Array localArray = Array.open(localStoreHandle);

        Assertions.assertArrayEquals(httpArray.metadata().shape, localArray.metadata().shape);
        Assertions.assertArrayEquals(httpArray.metadata().chunkShape(), localArray.metadata().chunkShape());

        ucar.ma2.Array httpData1 = httpArray.read(new long[]{0, 0, 0, 0}, new int[]{1, 64, 64, 64});
        ucar.ma2.Array localData1 = localArray.read(new long[]{0, 0, 0, 0}, new int[]{1, 64, 64, 64});

        assert MultiArrayUtils.allValuesEqual(httpData1, localData1);

        //offset to where l4_sample contains non-zero values
        long[] offset = new long[4];
        long[] originalOffset = new long[]{0, 3073, 3073, 513};
        long[] originalShape = new long[]{1, 4096, 4096, 2048};
        long[] arrayShape = httpArray.metadata().shape;
        for (int i = 0; i < 4; i++) {
            offset[i] = originalOffset[i] / (originalShape[i] / arrayShape[i]);
        }

        ucar.ma2.Array httpData2 = httpArray.read(offset, new int[]{1, 64, 64, 64});
        ucar.ma2.Array localData2 = localArray.read(offset, new int[]{1, 64, 64, 64});

        assert MultiArrayUtils.allValuesEqual(httpData2, localData2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testParallel(boolean useParallel) throws IOException, ZarrException {
        int[] testData = new int[512 * 512 * 512];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testParallelRead");
        ArrayMetadata metadata = Array.metadataBuilder()
            .withShape(512, 512, 512)
            .withDataType(DataType.UINT32)
            .withChunkShape(100, 100, 100)
            .withFillValue(0)
            .build();
        Array writeArray = Array.create(storeHandle, metadata);
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{512, 512, 512}, testData), useParallel);

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read(useParallel);

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
        clearTestoutputFolder();
    }

    @Test
    public void testMetadataAcceptsEmptyStorageTransformer() throws ZarrException, IOException {
        // non-empty storage transformers are currently not supported

        Map<String, Object>[] storageTransformersEmpty = Array.open(
            new FilesystemStore(TESTDATA).resolve("storage_transformer", "empty")
        ).metadata().storageTransformers;
        assert storageTransformersEmpty.length == 0;

        assertThrows(JsonMappingException.class, () -> Array.open(
            new FilesystemStore(TESTDATA).resolve("storage_transformer", "exists"))
        );

        ArrayMetadataBuilder builderWithStorageTransformer = Array.metadataBuilder()
            .withShape(1)
            .withChunkShape(1)
            .withDataType(DataType.UINT8)
            .withStorageTransformers(new HashMap[]{new HashMap<String, Object>() {{
                put("some", "value");
            }}});

        assertThrows(ZarrException.class, () -> Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("storage_transformer"),
            builderWithStorageTransformer.build()
        ));
    }

    @Test
    public void testOpen() throws ZarrException, IOException {
        StoreHandle arrayHandle = new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1");
        StoreHandle groupHandle = new FilesystemStore(TESTDATA).resolve("l4_sample");
        StoreHandle v2Handle = new FilesystemStore(TESTDATA).resolve("v2_sample");

        Array array = (Array) Node.open(arrayHandle);
        Assertions.assertEquals(4, array.metadata().shape.length);

        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayHandle);
        Assertions.assertEquals(4, array.metadata().shape.length);

        array = (Array) dev.zarr.zarrjava.core.Node.open(arrayHandle);
        Assertions.assertEquals(4, array.metadata().shape.length);

        Group group = (Group) Node.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        group = (Group) dev.zarr.zarrjava.core.Group.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        group = (Group) dev.zarr.zarrjava.core.Node.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(TESTDATA.resolve("non_existing")));
        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(v2Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> Group.open(v2Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> Array.open(v2Handle));
    }

    @Test
    public void testOpenOverloads() throws ZarrException, IOException {
        Path arrayPath = TESTDATA.resolve("l4_sample").resolve("color").resolve("1");
        Path groupPath = TESTDATA.resolve("l4_sample");
        Path v2GroupPath = TESTDATA.resolve("v2_sample");

        Array array = (Array) Node.open(arrayPath);
        Assertions.assertEquals(4, array.metadata().shape.length);
        array = (Array) Node.open(arrayPath.toString());
        Assertions.assertEquals(4, array.metadata().shape.length);

        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayPath);
        Assertions.assertEquals(4, array.metadata().shape.length);
        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayPath.toString());
        Assertions.assertEquals(4, array.metadata().shape.length);

        array = (Array) dev.zarr.zarrjava.core.Node.open(arrayPath);
        Assertions.assertEquals(4, array.metadata().shape.length);
        array = (Array) dev.zarr.zarrjava.core.Node.open(arrayPath.toString());
        Assertions.assertEquals(4, array.metadata().shape.length);

        Group group = (Group) Node.open(groupPath);
        Assertions.assertInstanceOf(Group.class, group.get("color"));
        group = (Group) Node.open(groupPath.toString());
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        group = (Group) dev.zarr.zarrjava.core.Group.open(groupPath);
        Assertions.assertInstanceOf(Group.class, group.get("color"));
        group = (Group) dev.zarr.zarrjava.core.Group.open(groupPath.toString());
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        group = (Group) dev.zarr.zarrjava.core.Node.open(groupPath);
        Assertions.assertInstanceOf(Group.class, group.get("color"));
        group = (Group) dev.zarr.zarrjava.core.Node.open(groupPath.toString());
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(TESTDATA.resolve("non_existing")));
        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(TESTDATA.resolve("non_existing").toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(v2GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(v2GroupPath.toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> Group.open(v2GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> Group.open(v2GroupPath.toString()));

        Assertions.assertThrows(NoSuchFileException.class, () -> Array.open(v2GroupPath));
        Assertions.assertThrows(NoSuchFileException.class, () -> Array.open(v2GroupPath.toString()));
    }

    @Test
    public void testGroup() throws IOException, ZarrException {
        FilesystemStore fsStore = new FilesystemStore(TESTOUTPUT);

        Attributes attributes = new Attributes();
        attributes.put("hello", "world");

        Group group = Group.create(fsStore.resolve("testgroup"));
        Group group2 = group.createGroup("test2", attributes);
        Array array = group2.createArray("array", b ->
            b.withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunkShape(5, 5)
        );
        array.write(new long[]{2, 2}, ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{8, 8}));

        Assertions.assertArrayEquals(new int[]{5, 5}, ((Array) ((Group) group.listAsArray()[0]).listAsArray()[0]).metadata().chunkShape());
    }

    @Test
    public void testCreateArray() throws ZarrException, IOException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testCreateArrayV3");
        Path storeHandlePath = TESTOUTPUT.resolve("testCreateArrayV3Path");
        String storeHandleString = String.valueOf(TESTOUTPUT.resolve("testCreateArrayV3String"));
        ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunkShape(5, 5)
                .build();

        Array.create(storeHandle, arrayMetadata);
        Assertions.assertTrue(storeHandle.resolve("zarr.json").exists());

        Array.create(storeHandlePath, arrayMetadata);
        Assertions.assertTrue(Files.exists(storeHandlePath.resolve("zarr.json")));

        Array.create(storeHandleString, arrayMetadata);
        Assertions.assertTrue(Files.exists(Paths.get(storeHandleString).resolve("zarr.json")));
    }

    @Test
    public void testCreateGroup() throws ZarrException, IOException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testCreateGroupV3");
        Path storeHandlePath = TESTOUTPUT.resolve("testCreateGroupV3Path");
        String storeHandleString = String.valueOf(TESTOUTPUT.resolve("testCreateGroupV3String"));
        Attributes attributes = new Attributes();
        attributes.put("hello", "world");

        Group group = Group.create(storeHandle, new GroupMetadata(attributes));
        Assertions.assertTrue(storeHandle.resolve("zarr.json").exists());
        Assertions.assertEquals("world", group.metadata.attributes().get("hello"));

        group = Group.create(storeHandlePath, new GroupMetadata(attributes));
        Assertions.assertTrue(Files.exists(storeHandlePath.resolve("zarr.json")));
        Assertions.assertEquals("world", group.metadata.attributes().get("hello"));

        group = Group.create(storeHandleString, new GroupMetadata(attributes));
        Assertions.assertTrue(Files.exists(Paths.get(storeHandleString).resolve("zarr.json")));
        Assertions.assertEquals("world", group.metadata.attributes().get("hello"));
    }

    @Test
    public void testAttributes() throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testAttributesV3");

        ArrayMetadata arrayMetadata = Array.metadataBuilder()
            .withShape(10, 10)
            .withDataType(DataType.UINT8)
            .withChunkShape(5, 5)
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
         assertContainsTestAttributes(arrayOpened.metadata().attributes());
         Assertions.assertEquals("attribute", arrayOpened.metadata().attributes().getString("specific"));
         Assertions.assertEquals("attribute", arrayOpened.metadata().attributes().getString("another"));
    }

    @Test
    public void testSetAndUpdateAttributes() throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testSetAttributesV3");

        ArrayMetadata arrayMetadata = Array.metadataBuilder()
            .withShape(10, 10)
            .withDataType(DataType.UINT8)
            .withChunkShape(5, 5)
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

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testResizeArrayV3");
        ArrayMetadata arrayMetadata = Array.metadataBuilder()
            .withShape(10, 10)
            .withDataType(DataType.UINT32)
            .withChunkShape(5, 5)
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
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testGroupAttributesV3");

        Group group = Group.create(storeHandle, new Attributes() {{
            put("group_attr", "group_value");
        }});

        Assertions.assertEquals("group_value", group.metadata().attributes().getString("group_attr"));

        group = Group.open(storeHandle);
        Assertions.assertEquals("group_value", group.metadata().attributes().getString("group_attr"));
    }
}
