package dev.zarr.zarrjava;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.core.Node;
import dev.zarr.zarrjava.store.*;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;
import dev.zarr.zarrjava.v3.codec.core.TransposeCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static dev.zarr.zarrjava.core.ArrayMetadata.parseFillValue;
import static dev.zarr.zarrjava.v3.Node.makeObjectMapper;
import static org.junit.Assert.assertThrows;

public class ZarrTest {

    final static Path TESTDATA = Paths.get("testdata");
    final static Path TESTOUTPUT = Paths.get("testoutput");

    @BeforeAll
    public static void clearTestoutputFolder() throws IOException {
        if (Files.exists(TESTOUTPUT)) {
            try (Stream<Path> walk = Files.walk(TESTOUTPUT)) {
                walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
        Files.createDirectory(TESTOUTPUT);
    }




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
    public void testCheckInvalidCodecConfiguration(Function<CodecBuilder, CodecBuilder> codecBuilder) throws Exception {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("invalid_codec_config", String.valueOf(codecBuilder.hashCode()));
        ArrayMetadataBuilder builder = Array.metadataBuilder()
            .withShape(new long[]{4, 4})
            .withDataType(DataType.UINT32)
            .withChunkShape(new int[]{2, 2})
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

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("invalid_chunksize");
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

    @ParameterizedTest
    @MethodSource("invalidShardSizes")
    public void testCheckShardingBounds(int[] shardSize) throws Exception {
        long[] shape = new long[]{10, 10};
        int[] innerChunkSize = new int[]{2, 2};

        ArrayMetadataBuilder builder = Array.metadataBuilder()
            .withShape(shape)
            .withDataType(DataType.UINT32).withChunkShape(shardSize);

        if (false) {
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
    @MethodSource("invalidChunkSizes")
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
    public void testFileSystemStores() throws IOException, ZarrException {
        FilesystemStore fsStore = new FilesystemStore(TESTDATA);
        ObjectMapper objectMapper = makeObjectMapper();

        GroupMetadata group = objectMapper.readValue(
            Files.readAllBytes(TESTDATA.resolve("l4_sample").resolve("zarr.json")),
            GroupMetadata.class
        );

        System.out.println(group);
        System.out.println(objectMapper.writeValueAsString(group));

        ArrayMetadata arrayMetadata = objectMapper.readValue(Files.readAllBytes(TESTDATA.resolve(
                "l4_sample").resolve("color").resolve("1").resolve("zarr.json")),
            ArrayMetadata.class);

        System.out.println(arrayMetadata);
        System.out.println(objectMapper.writeValueAsString(arrayMetadata));

        System.out.println(
            Array.open(fsStore.resolve("l4_sample", "color", "1")));
        System.out.println(
            Arrays.toString(Group.open(fsStore.resolve("l4_sample")).list().toArray(Node[]::new)));
        System.out.println(
            Arrays.toString(((Group) Group.open(fsStore.resolve("l4_sample")).get("color")).list()
                .toArray(Node[]::new)));
    }

    @Test
    public void testS3Store() throws IOException, ZarrException {
        S3Store s3Store = new S3Store(AmazonS3ClientBuilder.standard()
            .withRegion("eu-west-1")
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build(), "static.webknossos.org", "data");
        System.out.println(Array.open(s3Store.resolve("zarr_v3", "l4_sample", "color", "1")));
    }

    @Test
    public void testV3ShardingReadCutout() throws IOException, ZarrException {
        Array array = Array.open(new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1"));

        ucar.ma2.Array outArray = array.read(new long[]{0, 3073, 3073, 513}, new int[]{1, 64, 64, 64});
        Assertions.assertEquals(outArray.getSize(), 64 * 64 * 64);
        Assertions.assertEquals(outArray.getByte(0), -98);
    }

    @Test
    public void testV3Access() throws IOException, ZarrException {
        Array readArray = Array.open(new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1"));

        ucar.ma2.Array outArray = readArray.access().withOffset(0, 3073, 3073, 513)
            .withShape(1, 64, 64, 64)
            .read();
        Assertions.assertEquals(outArray.getSize(), 64 * 64 * 64);
        Assertions.assertEquals(outArray.getByte(0), -98);

        Array writeArray = Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("l4_sample_2", "color", "1"),
            readArray.metadata
        );
        writeArray.access().withOffset(0, 3073, 3073, 513).write(outArray);
    }

    @ParameterizedTest
    @ValueSource(strings = {"start", "end"})
    public void testV3ShardingReadWrite(String indexLocation) throws IOException, ZarrException {
        Array readArray = Array.open(
            new FilesystemStore(TESTDATA).resolve("sharding_index_location", indexLocation));
        ucar.ma2.Array readArrayContent = readArray.read();
        Array writeArray = Array.create(
            new FilesystemStore(TESTOUTPUT).resolve("sharding_index_location", indexLocation),
            readArray.metadata
        );
        writeArray.write(readArrayContent);
        ucar.ma2.Array outArray = writeArray.read();

        assert MultiArrayUtils.allValuesEqual(readArrayContent, outArray);
    }

    @Test
    public void testV3Codecs() throws IOException, ZarrException {
        int[] readShape = new int[]{1, 1, 1024, 1024};
        Array readArray = Array.open(
            new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "8-8-2"));
        ucar.ma2.Array readArrayContent = readArray.read(new long[4], readShape);
        {
            Array gzipArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("l4_sample_gzip", "color", "8-8-2"),
                Array.metadataBuilder(readArray.metadata).withCodecs(c -> c.withGzip(5)).build()
            );
            gzipArray.write(readArrayContent);
            ucar.ma2.Array outGzipArray = gzipArray.read(new long[4], readShape);
            assert MultiArrayUtils.allValuesEqual(outGzipArray, readArrayContent);
        }
        {
            Array bloscArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("l4_sample_blosc", "color", "8-8-2"),
                Array.metadataBuilder(readArray.metadata).withCodecs(c -> c.withBlosc("zstd", 5)).build()
            );
            bloscArray.write(readArrayContent);
            ucar.ma2.Array outBloscArray = bloscArray.read(new long[4], readShape);
            assert MultiArrayUtils.allValuesEqual(outBloscArray, readArrayContent);
        }
        {
            Array zstdArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("l4_sample_zstd", "color", "8-8-2"),
                Array.metadataBuilder(readArray.metadata).withCodecs(c -> c.withZstd(10)).build()
            );
            zstdArray.write(readArrayContent);
            ucar.ma2.Array outZstdArray = zstdArray.read(new long[4], readShape);
            assert MultiArrayUtils.allValuesEqual(outZstdArray, readArrayContent);
        }
    }

    @Test
    public void testV3ArrayMetadataBuilder() throws ZarrException {
        Array.metadataBuilder()
            .withShape(1, 4096, 4096, 1536)
            .withDataType(DataType.UINT32)
            .withChunkShape(1, 1024, 1024, 1024)
            .withFillValue(0)
            .withCodecs(
                c -> c.withSharding(new int[]{1, 32, 32, 32}, CodecBuilder::withBlosc))
            .build();
    }

    @Test
    public void testV3FillValue() throws ZarrException {
        Assertions.assertEquals((int) parseFillValue(0, DataType.UINT32), 0);
        Assertions.assertEquals((int) parseFillValue("0x00010203", DataType.UINT32), 50462976);
        Assertions.assertEquals((byte) parseFillValue("0b00000010", DataType.UINT8), 2);
        assert Double.isNaN((double) parseFillValue("NaN", DataType.FLOAT64));
        assert Double.isInfinite((double) parseFillValue("-Infinity", DataType.FLOAT64));
    }

    @Test
    public void testV3Group() throws IOException, ZarrException {
        FilesystemStore fsStore = new FilesystemStore(TESTOUTPUT);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("hello", "world");

        Group group = Group.create(fsStore.resolve("testgroup"));
        Group group2 = group.createGroup("test2", attributes);
        Array array = group2.createArray("array", b ->
            b.withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunkShape(5, 5)
        );
        array.write(new long[]{2, 2}, ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{8, 8}));

        Assertions.assertArrayEquals(((Array) ((Group) group.listAsArray()[0]).listAsArray()[0]).metadata.chunkShape(), new int[]{5, 5});
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
        System.out.println(httpArray);
        System.out.println(localArray);

        ucar.ma2.Array httpData1 = httpArray.read(new long[]{0, 0, 0, 0}, new int[]{1, 64, 64, 64});
        ucar.ma2.Array localData1 = localArray.read(new long[]{0, 0, 0, 0}, new int[]{1, 64, 64, 64});

        assert MultiArrayUtils.allValuesEqual(httpData1, localData1);

        //offset to where l4_sample contains non-zero values
        long[] offset = new long[4];
        long[] originalOffset = new long[]{0, 3073, 3073, 513};
        long[] originalShape = new long[]{1, 4096, 4096, 2048};
        long[] arrayShape = httpArray.metadata.shape;
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
        ).metadata.storageTransformers;
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
    public void testV2create() throws IOException, ZarrException {
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
    public void testV2createZlib(int level) throws IOException, ZarrException {
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
    public void testV2noFillValue(dev.zarr.zarrjava.v2.DataType dataType) throws IOException, ZarrException {
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
    public void testV2Group() throws IOException, ZarrException {
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

    @Test
    public void testGenericOpenV3() throws ZarrException, IOException {
        StoreHandle arrayHandle = new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1");
        StoreHandle groupHandle = new FilesystemStore(TESTDATA).resolve("l4_sample");
        StoreHandle v2Handle = new FilesystemStore(TESTDATA).resolve("v2_sample");

        Array array = (Array) Node.open(arrayHandle);
        Assertions.assertEquals(4, (array).metadata.shape.length);

        array = (Array) dev.zarr.zarrjava.core.Array.open(arrayHandle);
        Assertions.assertEquals(4, (array).metadata.shape.length);

        array = (Array) dev.zarr.zarrjava.v3.Node.open(arrayHandle);
        Assertions.assertEquals(4, (array).metadata.shape.length);

        Group group = (Group) Node.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        group = (Group) dev.zarr.zarrjava.core.Group.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        group = (Group) dev.zarr.zarrjava.v3.Node.open(groupHandle);
        Assertions.assertInstanceOf(Group.class, group.get("color"));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(new FilesystemStore(TESTDATA).resolve("non_existing")));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Node.open(v2Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Group.open(v2Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v3.Array.open(v2Handle));
    }

    @Test
    public void testGenericOpenV2() throws ZarrException, IOException {
        StoreHandle arrayHandle = new FilesystemStore(TESTDATA).resolve("v2_sample", "subgroup", "array");
        StoreHandle groupHandle = new FilesystemStore(TESTDATA).resolve("v2_sample");
        StoreHandle v3Handle = new FilesystemStore(TESTDATA).resolve("l4_sample");

        dev.zarr.zarrjava.v2.Array array = (dev.zarr.zarrjava.v2.Array) Node.open(arrayHandle);
        Assertions.assertEquals(3, (array).metadata.shape.length);

        array = (dev.zarr.zarrjava.v2.Array) dev.zarr.zarrjava.core.Array.open(arrayHandle);
        Assertions.assertEquals(3, (array).metadata.shape.length);

        array = (dev.zarr.zarrjava.v2.Array) dev.zarr.zarrjava.v2.Node.open(arrayHandle);
        Assertions.assertEquals(3, (array).metadata.shape.length);

        dev.zarr.zarrjava.v2.Group group = (dev.zarr.zarrjava.v2.Group) Node.open(groupHandle);
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));

        group = (dev.zarr.zarrjava.v2.Group) dev.zarr.zarrjava.core.Group.open(groupHandle);
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));

        group = (dev.zarr.zarrjava.v2.Group) dev.zarr.zarrjava.v2.Node.open(groupHandle);
        Assertions.assertInstanceOf(dev.zarr.zarrjava.v2.Group.class, group.get("subgroup"));

        Assertions.assertThrows(NoSuchFileException.class, () -> Node.open(new FilesystemStore(TESTDATA).resolve("non_existing")));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Node.open(v3Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Group.open(v3Handle));
        Assertions.assertThrows(NoSuchFileException.class, () -> dev.zarr.zarrjava.v2.Array.open(v3Handle));
    }
}
