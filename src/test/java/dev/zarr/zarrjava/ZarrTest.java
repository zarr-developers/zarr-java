package dev.zarr.zarrjava;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.store.S3Store;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import dev.zarr.zarrjava.v3.codec.core.TransposeCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import ucar.ma2.MAMath;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.*;

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
    @ValueSource(strings = {"blosc", "gzip", "zstd", "bytes", "transpose", "sharding_start", "sharding_end", "crc32c"})
    public void testReadFromZarrita(String codec) throws IOException, ZarrException, InterruptedException {

        String command = pythonPath();
        ProcessBuilder pb = new ProcessBuilder(command, PYTHON_TEST_PATH.resolve("zarrita_write.py").toString(), codec, TESTOUTPUT.toString());
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

        Array array = Array.open(new FilesystemStore(TESTOUTPUT).resolve("read_from_zarrita", codec));
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
    @ValueSource(strings = {"blosc", "gzip", "zstd", "bytes", "transpose", "sharding_start", "sharding_end", "crc32c"})
    public void testWriteRead(String codec) throws IOException, ZarrException, InterruptedException {
        int[] testData = new int[16 * 16 * 16];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("write_to_zarrita", codec);
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 16)
                .withDataType(DataType.UINT32)
                .withChunkShape(2, 4, 8)
                .withFillValue(0)
                .withAttributes(Map.of("test_key", "test_value"));

        switch (codec) {
            case "blosc":
                builder = builder.withCodecs(CodecBuilder::withBlosc);
                break;
            case "gzip":
                builder = builder.withCodecs(CodecBuilder::withGzip);
                break;
            case "zstd":
                builder = builder.withCodecs(c -> c.withZstd(0));
                break;
            case "bytes":
                builder = builder.withCodecs(c -> c.withBytes("LITTLE"));
                break;
            case "transpose":
                builder = builder.withCodecs(c -> c.withTranspose(new int[]{1, 0, 2}));
                break;
            case "sharding_start":
                builder = builder.withCodecs(c -> c.withSharding(new int[]{2, 2, 4}, c1 -> c1.withBytes("LITTLE"), "start"));
                break;
            case "sharding_end":
                builder = builder.withCodecs(c -> c.withSharding(new int[]{2, 2, 4}, c1 -> c1.withBytes("LITTLE"), "end"));
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

        ProcessBuilder pb = new ProcessBuilder(command, PYTHON_TEST_PATH.resolve("zarrita_read.py").toString(), codec, TESTOUTPUT.toString());
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

        ArrayMetadata.CoreArrayMetadata metadata = new ArrayMetadata.CoreArrayMetadata(
                new long[]{2, 3, 3},
                new int[]{2, 3, 3},
                DataType.UINT32,
                null);
        TransposeCodec transposeCodec = new TransposeCodec(new TransposeCodec.Configuration(new int[]{1, 2, 0}));
        TransposeCodec transposeCodecWrongOrder1 = new TransposeCodec(new TransposeCodec.Configuration(new int[]{1, 2, 2}));
        TransposeCodec transposeCodecWrongOrder2 = new TransposeCodec(new TransposeCodec.Configuration(new int[]{1, 2, 3}));
        TransposeCodec transposeCodecWrongOrder3 = new TransposeCodec(new TransposeCodec.Configuration(new int[]{1, 2, 3, 0}));
        transposeCodec.setCoreArrayMetadata(metadata);
        transposeCodecWrongOrder1.setCoreArrayMetadata(metadata);
        transposeCodecWrongOrder2.setCoreArrayMetadata(metadata);
        transposeCodecWrongOrder3.setCoreArrayMetadata(metadata);

        assert MAMath.equals(testDataTransposed120, transposeCodec.encode(testData));
        assert MAMath.equals(testData, transposeCodec.decode(testDataTransposed120));
        assertThrows(ZarrException.class, () -> transposeCodecWrongOrder1.encode(testData));
        assertThrows(ZarrException.class, () -> transposeCodecWrongOrder2.encode(testData));
        assertThrows(ZarrException.class, () -> transposeCodecWrongOrder3.encode(testData));
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

        Group group = Group.create(fsStore.resolve("testgroup"));
        Group group2 = group.createGroup("test2", Map.of("hello", "world"));
        Array array = group2.createArray("array", b ->
                b.withShape(10, 10)
                        .withDataType(DataType.UINT8)
                        .withChunkShape(5, 5)
        );
        array.write(new long[]{2, 2}, ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{8, 8}));

        Assertions.assertArrayEquals(((Array) ((Group) group.listAsArray()[0]).listAsArray()[0]).metadata.chunkShape(), new int[]{5, 5});
    }

    @Test
    public void testV2() throws IOException{
        FilesystemStore fsStore = new FilesystemStore("");
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");

        System.out.println(dev.zarr.zarrjava.v2.Array.open(httpStore.resolve("l4_sample", "color", "1")));
    }


}
