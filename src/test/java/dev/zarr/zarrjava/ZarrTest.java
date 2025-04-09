package dev.zarr.zarrjava;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import dev.zarr.zarrjava.store.*;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.v3.codec.Codec;
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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.Assert.assertThrows;

public class ZarrTest {

    final static Path TESTDATA = Paths.get("testdata");
    final static Path TESTOUTPUT = Paths.get("testoutput");
    final static Path PYTHON_TEST_PATH = Paths.get("src/test/python-scripts/");

    public static String pythonPath() {
        if (System.getProperty("os.name").startsWith("Windows")) {
            return "venv_zarrita\\Scripts\\python.exe";
        }
        return "venv_zarrita/bin/python";
    }

    @BeforeAll
    public static void clearTestoutputFolder() throws IOException {
        if (Files.exists(TESTOUTPUT)) {
            try (Stream<Path> walk = Files.walk(TESTOUTPUT)) {
                walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
        Files.createDirectory(TESTOUTPUT);
    }

    @ParameterizedTest
    @CsvSource({
            "blosc,blosclz_noshuffle_0", "blosc,lz4_shuffle_6", "blosc,lz4hc_bitshuffle_3", "blosc,zlib_shuffle_5", "blosc,zstd_bitshuffle_9",
            "gzip,0", "gzip,5",
            "zstd,0_true", "zstd,5_true", "zstd,0_false", "zstd,5_false",
            "bytes,BIG", "bytes,LITTLE",
            "transpose,_",
            "sharding,start", "sharding,end",
            "sharding_nested,_",
            "crc32c,_",
    })

    public void testReadFromZarrita(String codec, String codecParam) throws IOException, ZarrException, InterruptedException {
        String command = pythonPath();
        ProcessBuilder pb = new ProcessBuilder(command, PYTHON_TEST_PATH.resolve("zarrita_write.py").toString(), codec, codecParam, TESTOUTPUT.toString());
        Process process = pb.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        BufferedReader readerErr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        while ((line = readerErr.readLine()) != null) {
            System.err.println(line);
        }

        int exitCode = process.waitFor();
        assert exitCode == 0;

        Array array = Array.open(new FilesystemStore(TESTOUTPUT).resolve("read_from_zarrita", codec, codecParam));
        ucar.ma2.Array result = array.read();

        //for expected values see zarrita_write.py
        Assertions.assertArrayEquals(new int[]{16, 16}, result.getShape());
        Assertions.assertEquals(DataType.INT32, array.metadata.dataType);
        Assertions.assertArrayEquals(new int[]{2, 8}, array.metadata.chunkShape());
        Assertions.assertEquals(42, array.metadata.attributes.get("answer"));

        int[] expectedData = new int[16 * 16];
        Arrays.setAll(expectedData, p -> p);
        Assertions.assertArrayEquals(expectedData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.INT));
    }

    @CsvSource({"0,true", "0,false", "5, true", "10, false"})
    @ParameterizedTest
    public void testZstdLibrary(int clevel, boolean checksumFlag) throws IOException, InterruptedException {
        //compress using ZstdCompressCtx
        int number = 123456;
        byte[] src = ByteBuffer.allocate(4).putInt(number).array();
        byte[] compressed;
        try (ZstdCompressCtx ctx = new ZstdCompressCtx()) {
            ctx.setLevel(clevel);
            ctx.setChecksum(checksumFlag);
            compressed = ctx.compress(src);
        }
        //decompress with Zstd.decompress
        long originalSize = Zstd.decompressedSize(compressed);
        byte[] decompressed = Zstd.decompress(compressed, (int) originalSize);
        Assertions.assertEquals(number, ByteBuffer.wrap(decompressed).getInt());

        //write compressed to file
        String compressedDataPath = TESTOUTPUT.resolve("compressed" + clevel + checksumFlag + ".bin").toString();
        try (FileOutputStream fos = new FileOutputStream(compressedDataPath)) {
            fos.write(compressed);
        }

        //decompress in python
        Process process = new ProcessBuilder(
                pythonPath(),
                PYTHON_TEST_PATH.resolve("zstd_decompress.py").toString(),
                compressedDataPath,
                Integer.toString(number)
        ).start();
        int exitCode = process.waitFor();
        assert exitCode == 0;
    }

    @ParameterizedTest
    @CsvSource({
            "blosc,blosclz_noshuffle_0", "blosc,lz4_shuffle_6", "blosc,lz4hc_bitshuffle_3", "blosc,zlib_shuffle_5", "blosc,zstd_bitshuffle_9",
            "gzip,0", "gzip,5",
            "zstd,0_true", "zstd,5_true", "zstd,0_false", "zstd,5_false",
            "bytes,BIG", "bytes,LITTLE",
            "transpose,_",
            "sharding,start", "sharding,end",
            "sharding_nested,_",
            "crc32c,_",
    })
    public void testWriteReadWithZarrita(String codec, String codecParam) throws Exception {
        int[] testData = new int[16 * 16 * 16];
        Arrays.setAll(testData, p -> p);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("test_key", "test_value");

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("write_to_zarrita", codec, codecParam);
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 16)
                .withDataType(DataType.UINT32)
                .withChunkShape(2, 4, 8)
                .withFillValue(0)
                .withAttributes(attributes);

        switch (codec) {
            case "blosc":
                String cname = codecParam.split("_")[0];
                String shuffle = codecParam.split("_")[1];
                int clevel_blosc = Integer.parseInt(codecParam.split("_")[2]);
                builder = builder.withCodecs(c -> c.withBlosc(cname, shuffle, clevel_blosc));
                break;
            case "gzip":
                builder = builder.withCodecs(c -> c.withGzip(Integer.parseInt(codecParam)));
                break;
            case "zstd":
                int clevel_zstd = Integer.parseInt(codecParam.split("_")[0]);
                boolean checksum = Boolean.parseBoolean(codecParam.split("_")[1]);
                builder = builder.withCodecs(c -> c.withZstd(clevel_zstd, checksum));
                break;
            case "bytes":
                builder = builder.withCodecs(c -> c.withBytes(codecParam));
                break;
            case "transpose":
                builder = builder.withCodecs(c -> c.withTranspose(new int[]{1, 0, 2}));
                break;
            case "sharding":
                builder = builder.withCodecs(c -> c.withSharding(new int[]{2, 2, 4}, c1 -> c1.withBytes("LITTLE"), codecParam));
                break;
            case "sharding_nested":
                builder = builder.withCodecs(c -> c.withSharding(new int[]{2, 2, 4}, c1 -> c1.withSharding(new int[]{2, 1, 2}, c2 -> c2.withBytes("LITTLE"))));
                break;
            case "crc32c":
                builder = builder.withCodecs(CodecBuilder::withCrc32c);
                break;
            default:
                throw new IllegalArgumentException("Invalid Codec: " + codec);
        }

        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{16, 16, 16}, testData));

        //read in zarr-java
        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        Assertions.assertEquals(DataType.UINT32, readArray.metadata.dataType);
        Assertions.assertArrayEquals(new int[]{2, 4, 8}, readArray.metadata.chunkShape());
        Assertions.assertEquals("test_value", readArray.metadata.attributes.get("test_key"));

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.INT));

        //read in zarrita
        String command = pythonPath();

        ProcessBuilder pb = new ProcessBuilder(command, PYTHON_TEST_PATH.resolve("zarrita_read.py").toString(), codec, codecParam, TESTOUTPUT.toString());
        Process process = pb.start();

        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }

        BufferedReader readerErr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        while ((line = readerErr.readLine()) != null) {
            System.err.println(line);
        }

        int exitCode = process.waitFor();
        assert exitCode == 0;
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

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.INT));
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

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.INT));
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
        ObjectMapper objectMapper = Node.makeObjectMapper();

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
    public void testHttpStore() throws IOException, ZarrException {
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data/");
        System.out.println(
                dev.zarr.zarrjava.v2.Array.open(httpStore.resolve("l4_sample", "color", "1")));
        System.out.println(Array.open(httpStore.resolve("zarr_v3", "l4_sample", "color", "1")));
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
        Assertions.assertEquals((int) ArrayMetadata.parseFillValue(0, DataType.UINT32), 0);
        Assertions.assertEquals((int) ArrayMetadata.parseFillValue("0x00010203", DataType.UINT32), 50462976);
        Assertions.assertEquals((byte) ArrayMetadata.parseFillValue("0b00000010", DataType.UINT8), 2);
        assert Double.isNaN((double) ArrayMetadata.parseFillValue("NaN", DataType.FLOAT64));
        assert Double.isInfinite((double) ArrayMetadata.parseFillValue("-Infinity", DataType.FLOAT64));
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
    public void testV2() throws IOException {
        FilesystemStore fsStore = new FilesystemStore("");
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");

        System.out.println(dev.zarr.zarrjava.v2.Array.open(httpStore.resolve("l4_sample", "color", "1")));
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
    @ValueSource(booleans = {false,true})
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

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.INT));
            clearTestoutputFolder();
    }
}

