package dev.zarr.zarrjava;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.store.S3Store;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.v3.codec.core.TransposeCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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
    //TODO: is the Path with / instead of \ readable in Windows?
    final static Path ZARRITA_WRITE_PATH = Paths.get("src/test/java/dev/zarr/zarrjava/zarrita_write.py");
    final static Path ZARRITA_READ_PATH = Paths.get("src/test/java/dev/zarr/zarrjava/zarrita_read.py");
    final static String CONDA_ENVIRONMENT = "zarrita_env";

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
    @ValueSource(strings = {"blosc", "gzip", "zstd", "bytes", "transpose", "sharding", "crc32c"})
    public void testReadFromZarrita(String codec) throws IOException, ZarrException, InterruptedException {

        String command = "venv_zarrita/bin/python";
        ProcessBuilder pb = new ProcessBuilder(command, ZARRITA_WRITE_PATH.toString(), codec, TESTOUTPUT.toString());
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

    //TODO: add crc32c
    @ParameterizedTest
    @ValueSource(strings = {"blosc", "gzip", "zstd", "bytes", "transpose", "sharding"})
    public void testWriteToZarrita(String codec) throws IOException, ZarrException, InterruptedException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("write_to_zarrita", codec);
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16)
                .withDataType(DataType.UINT32)
                .withChunkShape(8, 8)
                .withFillValue(0);

        switch (codec) {
            case "blosc":
                builder = builder.withCodecs(c -> c.withBlosc());
                break;
            case "gzip":
                builder = builder.withCodecs(c -> c.withGzip());
                break;
            case "zstd":
                builder = builder.withCodecs(c -> c.withZstd(0));
                break;
            case "bytes":
                builder = builder.withCodecs(c -> c.withBytes("LITTLE"));
                break;
            case "transpose":
                builder = builder.withCodecs(c -> c.withTranspose(new int[]{1, 0}));
                break;
            case "sharding":
                builder = builder.withCodecs(c -> c.withSharding(new int[]{4, 4}, c1 -> c1.withBytes("LITTLE")));
                break;
            case "crc32c":
                //missing
                break;
            default:
                throw new IllegalArgumentException("Invalid Codec: " + codec);
        }

        Array array = Array.create(storeHandle, builder.build());

        int[] data = new int[16 * 16];
        Arrays.setAll(data, p -> p);
        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{16, 16}, data));

        String command = "zarrita/bin/python";

        ProcessBuilder pb = new ProcessBuilder(command, ZARRITA_READ_PATH.toString(), codec, TESTOUTPUT.toString());
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
        //TODO return metadata from zarrita_read.py and do assertions here
    }


    @ParameterizedTest
    @ValueSource(strings = {"blosc", "gzip", "zstd", "bytes", "transpose", "sharding"})
    public void testCodecsWriteRead(String codec) throws IOException, ZarrException, InterruptedException {
        int[] testData = new int[16 * 16 * 16];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testWriteAndRead3d", codec);
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 16)
                .withDataType(DataType.UINT32)
                .withChunkShape(2, 4, 8)
                .withFillValue(0)
                .withAttributes(Map.of("test_key", "test_value"));

        switch (codec) {
            case "blosc":
                builder = builder.withCodecs(c -> c.withBlosc());
                break;
            case "gzip":
                builder = builder.withCodecs(c -> c.withGzip());
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
            case "sharding":
                builder = builder.withCodecs(c -> c.withSharding(new int[]{2, 2, 4}, c1 -> c1.withBytes("LITTLE")));
                break;
            case "crc32c":
                //missing
                break;
            default:
                throw new IllegalArgumentException("Invalid Codec: " + codec);
        }

        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{16, 16, 16}, testData));

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        Assertions.assertEquals(DataType.UINT32, readArray.metadata.dataType);
        Assertions.assertArrayEquals(new int[]{2, 4, 8}, readArray.metadata.chunkShape());
        Assertions.assertEquals("test_value", readArray.metadata.attributes.get("test_key"));

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.INT));
    }

    @Test
    public void testCodecTranspose() throws IOException, ZarrException, InterruptedException {
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

        assert ucar.ma2.MAMath.equals(testDataTransposed120, transposeCodec.encode(testData));
        assert ucar.ma2.MAMath.equals(testData, transposeCodec.decode(testDataTransposed120));
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
        assertEquals(outArray.getSize(), 64 * 64 * 64);
        assertEquals(outArray.getByte(0), -98);
    }

    @Test
    public void testV3Access() throws IOException, ZarrException {
        Array readArray = Array.open(new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1"));

        ucar.ma2.Array outArray = readArray.access().withOffset(0, 3073, 3073, 513)
                .withShape(1, 64, 64, 64)
                .read();
        assertEquals(outArray.getSize(), 64 * 64 * 64);
        assertEquals(outArray.getByte(0), -98);

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
                        c -> c.withSharding(new int[]{1, 32, 32, 32}, c1 -> c1.withBlosc()))
                .build();
    }

    @Test
    public void testV3FillValue() throws ZarrException {
        assertEquals((int) ArrayMetadata.parseFillValue(0, DataType.UINT32), 0);
        assertEquals((int) ArrayMetadata.parseFillValue("0x00010203", DataType.UINT32), 50462976);
        assertEquals((byte) ArrayMetadata.parseFillValue("0b00000010", DataType.UINT8), 2);
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

        assertArrayEquals(
                ((Array) ((Group) group.listAsArray()[0]).listAsArray()[0]).metadata.chunkShape(),
                new int[]{5, 5});
    }

    @Test
    public void testV2() throws IOException, ZarrException {
        FilesystemStore fsStore = new FilesystemStore("");
        HttpStore httpStore = new HttpStore("https://static.webknossos.org/data");

        System.out.println(
                dev.zarr.zarrjava.v2.Array.open(httpStore.resolve("l4_sample", "color", "1")));
    }


}
