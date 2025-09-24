package dev.zarr.zarrjava;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadataBuilder;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class ZarrPythonTests {

    final static Path TESTOUTPUT = Paths.get("testoutput");
    final static Path PYTHON_TEST_PATH = Paths.get("src/test/python-scripts/");

    @BeforeAll
    public static void clearTestoutputFolder() throws IOException {
        if (Files.exists(TESTOUTPUT)) {
            try (Stream<Path> walk = Files.walk(TESTOUTPUT)) {
                walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
        Files.createDirectory(TESTOUTPUT);
    }

    public static int runCommand(String... command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder();
        pb.command().addAll(Arrays.asList(command));
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

        return process.waitFor();
    }

    @BeforeAll
    public static void setupUV() {
        try {
            int exitCode = runCommand("uv", "version");
            if (exitCode != 0) {
                //setup uv
                assert runCommand("uv", "venv") == 0;
                assert runCommand("uv", "init") == 0;
                assert runCommand("uv", "add", "zarr") == 0;
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("uv not installed or not in PATH. See");
        }
    }

    public void run_python_script(String scriptName, String... args) throws IOException, InterruptedException {
        int exitCode = runCommand(Stream.concat(Stream.of("uv", "run", PYTHON_TEST_PATH.resolve(scriptName)
            .toString()), Arrays.stream(args)).toArray(String[]::new));
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
    public void testReadFromZarrPythonV3(String codec, String codecParam) throws IOException, ZarrException, InterruptedException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("read_from_zarr_python", codec, codecParam);
        run_python_script("zarr_python_write.py", codec, codecParam, storeHandle.toPath().toString());
        Array array = Array.open(storeHandle);
        ucar.ma2.Array result = array.read();

        //for expected values see zarr_python_write.py
        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        Assertions.assertEquals(DataType.INT32, array.metadata.dataType);
        Assertions.assertArrayEquals(new int[]{2, 4, 8}, array.metadata.chunkShape());
        Assertions.assertEquals(42, array.metadata.attributes.get("answer"));

        int[] expectedData = new int[16 * 16 * 16];
        Arrays.setAll(expectedData, p -> p);
        Assertions.assertArrayEquals(expectedData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.INT));
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
    public void testWriteReadWithZarrPythonV3(String codec, String codecParam) throws Exception {
        int[] testData = new int[16 * 16 * 16];
        Arrays.setAll(testData, p -> p);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("test_key", "test_value");
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("write_to_zarr_python", codec, codecParam);

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

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));

        //read in zarr_python
        run_python_script("zarr_python_read.py", codec, codecParam, storeHandle.toPath().toString());
    }


    @ParameterizedTest
    @CsvSource({
        "zlib,0", "zlib,5",
        "blosc,blosclz_noshuffle_0", "blosc,lz4_shuffle_6", "blosc,lz4hc_bitshuffle_3", "blosc,zlib_shuffle_5", "blosc,zstd_bitshuffle_9",
    })
    public void testReadFromZarrPythonV2(String compressor, String compressorParam) throws IOException, ZarrException, InterruptedException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("read_from_zarr_python_v2", compressor, compressorParam);
        run_python_script("zarr_python_write_v2.py", compressor, compressorParam, storeHandle.toPath().toString());

        dev.zarr.zarrjava.v2.Array array = dev.zarr.zarrjava.v2.Array.open(storeHandle);
        ucar.ma2.Array result = array.read();

        //for expected values see zarr_python_write.py
        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        Assertions.assertEquals(dev.zarr.zarrjava.v2.DataType.INT32, array.metadata.dataType);
        Assertions.assertArrayEquals(new int[]{2, 4, 8}, array.metadata.chunkShape());
//        Assertions.assertEquals(42, array.metadata.attributes.get("answer"));

        int[] expectedData = new int[16 * 16 * 16];
        Arrays.setAll(expectedData, p -> p);
        Assertions.assertArrayEquals(expectedData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.INT));
    }


    @ParameterizedTest
    @CsvSource({
        "zlib,0", "zlib,5",
        "blosc,blosclz_noshuffle_0", "blosc,lz4_shuffle_6", "blosc,lz4hc_bitshuffle_3", "blosc,zlib_shuffle_5", "blosc,zstd_bitshuffle_9",
    })
    public void testWriteReadWithZarrPythonV2(String compressor, String compressorParam) throws Exception {
        int[] testData = new int[16 * 16 * 16];
        Arrays.setAll(testData, p -> p);

//        Map<String, Object> attributes = new HashMap<>();
//        attributes.put("test_key", "test_value");
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("write_to_zarr_python_v2", compressor, compressorParam);

        dev.zarr.zarrjava.v2.ArrayMetadataBuilder builder = dev.zarr.zarrjava.v2.Array.metadataBuilder()
            .withShape(16, 16, 16)
            .withDataType(dev.zarr.zarrjava.v2.DataType.UINT32)
            .withChunks(2, 4, 8)
//            .withAttributes(attributes)
            .withFillValue(0);

        switch (compressor) {
            case "blosc":
                String cname = compressorParam.split("_")[0];
                String shuffle = compressorParam.split("_")[1];
                int clevel_blosc = Integer.parseInt(compressorParam.split("_")[2]);
                builder = builder.withBloscCompressor(cname, shuffle, clevel_blosc);
                break;
            case "zlib":
                builder = builder.withZlibCompressor(Integer.parseInt(compressorParam));
                break;
            default:
                throw new IllegalArgumentException("Invalid compressor: " + compressor);
        }

        dev.zarr.zarrjava.v2.Array writeArray = dev.zarr.zarrjava.v2.Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{16, 16, 16}, testData));

        //read in zarr-java
        dev.zarr.zarrjava.v2.Array readArray = dev.zarr.zarrjava.v2.Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        Assertions.assertEquals(dev.zarr.zarrjava.v2.DataType.UINT32, readArray.metadata.dataType);
        Assertions.assertArrayEquals(new int[]{2, 4, 8}, readArray.metadata.chunkShape());
//        Assertions.assertEquals("test_value", readArray.metadata.attributes.get("test_key"));

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));

        //read in zarr_python
        run_python_script("zarr_python_read_v2.py", compressor, compressorParam, storeHandle.toPath().toString());
    }
}
