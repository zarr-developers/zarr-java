package dev.zarr.zarrjava;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.Group;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadataBuilder;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;


public class ZarrPythonTests extends ZarrTest {

    final static Path PYTHON_TEST_PATH = Paths.get("src/test/python-scripts/");


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
                assert runCommand("uv", "add", "zarr", "zstandard") == 0;
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

        static ucar.ma2.Array testdata(dev.zarr.zarrjava.core.DataType dt){
        ucar.ma2.DataType ma2Type = dt.getMA2DataType();
        ucar.ma2.Array array =  ucar.ma2.Array.factory(ma2Type, new int[]{16, 16, 16});
        for (int i = 0; i < array.getSize(); i++) {
            switch (ma2Type) {
                case BOOLEAN:
                    array.setBoolean(i, i%2 == 0);
                    break;
                case BYTE:
                case UBYTE:
                    array.setByte(i, (byte) i);
                    break;
                case SHORT:
                case USHORT:
                    array.setShort(i, (short) i);
                    break;
                case INT:
                    array.setInt(i, i);
                    break;
                case UINT:
                    array.setLong(i, i & 0xFFFFFFFFL);
                    break;
                case LONG:
                case ULONG:
                    array.setLong(i, (long) i);
                    break;
                case FLOAT:
                    array.setFloat(i, (float) i);
                    break;
                case DOUBLE:
                    array.setDouble(i, (double) i);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid DataType: " + dt);
            }
        }
        return array;
    }

    static void assertIsTestdata(ucar.ma2.Array result, dev.zarr.zarrjava.core.DataType dt) {
        // expected values are i for index i
        ucar.ma2.DataType ma2Type = dt.getMA2DataType();
        for (int i = 0; i < result.getSize(); i++) {
            switch (ma2Type) {
                case BOOLEAN:
                    Assertions.assertEquals(i % 2 == 0, result.getBoolean(i));
                    break;
                case BYTE:
                case UBYTE:
                    Assertions.assertEquals((byte) i, result.getByte(i));
                    break;
                case SHORT:
                case USHORT:
                    Assertions.assertEquals((short) i, result.getShort(i));
                    break;
                case INT:
                    Assertions.assertEquals(i, result.getInt(i));
                    break;
                case UINT:
                    Assertions.assertEquals(i & 0xFFFFFFFFL, result.getLong(i));
                    break;
                case LONG:
                case ULONG:
                    Assertions.assertEquals((long) i, result.getLong(i));
                    break;
                case FLOAT:
                    Assertions.assertEquals((float) i, result.getFloat(i), 1e-6);
                    break;
                case DOUBLE:
                    Assertions.assertEquals((double) i, result.getDouble(i), 1e-12);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid DataType: " + dt);
            }
        }
    }

    static Stream<Object[]> compressorAndDataTypeProviderV3() {
        Stream<Object[]> datatypeTests = Stream.of(
//            DataType.BOOL,
//            DataType.INT8,
//            DataType.UINT8, // -> BUG: see https://github.com/zarr-developers/zarr-java/issues/27
            DataType.INT16,
            DataType.UINT16,
            DataType.INT32,
            DataType.UINT32,
            DataType.INT64,
            DataType.UINT64,
            DataType.FLOAT32,
            DataType.FLOAT64
        ).flatMap(dt -> Stream.of(
            new Object[]{"sharding", "end", dt},
            new Object[]{"blosc", "blosclz_shuffle_3", dt}
        ));

        Stream<Object[]> codecsTests = Stream.of(
            new Object[]{"blosc", "blosclz_noshuffle_0", DataType.INT32},
            new Object[]{"blosc", "lz4_shuffle_6", DataType.INT32},
            new Object[]{"blosc", "lz4hc_bitshuffle_3", DataType.INT32},
            new Object[]{"blosc", "zlib_shuffle_5", DataType.INT32},
            new Object[]{"blosc", "zstd_bitshuffle_9", DataType.INT32},
            new Object[]{"gzip", "0", DataType.INT32},
            new Object[]{"gzip", "5", DataType.INT32},
            new Object[]{"zstd", "0_true", DataType.INT32},
            new Object[]{"zstd", "5_true", DataType.INT32},
            new Object[]{"zstd", "0_false", DataType.INT32},
            new Object[]{"zstd", "5_false", DataType.INT32},
            new Object[]{"bytes", "BIG", DataType.INT32},
            new Object[]{"bytes", "LITTLE", DataType.INT32},
            new Object[]{"transpose", "_", DataType.INT32},
            new Object[]{"sharding", "start", DataType.INT32},
            new Object[]{"sharding_nested", "_", DataType.INT32},
            new Object[]{"crc32c", "_", DataType.INT32}
        );

        return Stream.concat(datatypeTests, codecsTests);
    }


    @ParameterizedTest
    @MethodSource("compressorAndDataTypeProviderV3")
    public void testReadV3(String codec, String codecParam, DataType dataType) throws IOException, ZarrException, InterruptedException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testReadV3", codec, codecParam, dataType.name());
        run_python_script("zarr_python_write.py", codec, codecParam, dataType.name().toLowerCase(), storeHandle.toPath().toString());
        Array array = Array.open(storeHandle);
        ucar.ma2.Array result = array.read();

        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        Assertions.assertEquals(dataType, array.metadata().dataType);
        Assertions.assertArrayEquals(new int[]{2, 4, 8}, array.metadata().chunkShape());
        Assertions.assertEquals(42, array.metadata().attributes.get("answer"));

        assertIsTestdata(result, DataType.INT32);
    }

    @ParameterizedTest
    @MethodSource("compressorAndDataTypeProviderV3")
    public void testWriteV3(String codec, String codecParam, DataType dataType) throws Exception {
        Attributes attributes = new Attributes();
        attributes.put("test_key", "test_value");
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testWriteV3", codec, codecParam, dataType.name());

        ArrayMetadataBuilder builder = Array.metadataBuilder()
            .withShape(16, 16, 16)
            .withDataType(dataType)
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
        writeArray.write(testdata(dataType));

        //read in zarr-java
        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        Assertions.assertEquals(dataType, readArray.metadata().dataType);
        Assertions.assertArrayEquals(new int[]{2, 4, 8}, readArray.metadata().chunkShape());
        Assertions.assertEquals("test_value", readArray.metadata().attributes.get("test_key"));

        assertIsTestdata(result, DataType.INT32);

        //read in zarr_python
        run_python_script("zarr_python_read.py", codec, codecParam, dataType.name().toLowerCase(), storeHandle.toPath().toString());
    }


    static Stream<Object[]> compressorAndDataTypeProviderV2() {
        Stream<Object[]> datatypeTests = Stream.of(
            dev.zarr.zarrjava.v2.DataType.BOOL,
            dev.zarr.zarrjava.v2.DataType.INT8,
            dev.zarr.zarrjava.v2.DataType.UINT8,
            dev.zarr.zarrjava.v2.DataType.INT16,
            dev.zarr.zarrjava.v2.DataType.UINT16,
            dev.zarr.zarrjava.v2.DataType.INT32,
            dev.zarr.zarrjava.v2.DataType.UINT32,
            dev.zarr.zarrjava.v2.DataType.INT64,
            dev.zarr.zarrjava.v2.DataType.UINT64,
            dev.zarr.zarrjava.v2.DataType.FLOAT32,
            dev.zarr.zarrjava.v2.DataType.FLOAT64
        ).flatMap(dt -> Stream.of(
            new Object[]{"zlib", "0", dt},
            new Object[]{"blosc", "blosclz_shuffle_3", dt}
        ));

        Stream <Object[]> bloscTests = Stream.of(
            new Object[]{"blosc", "blosclz_noshuffle_0", dev.zarr.zarrjava.v2.DataType.INT32},
            new Object[]{"blosc", "lz4_shuffle_6", dev.zarr.zarrjava.v2.DataType.INT32},
            new Object[]{"blosc", "lz4hc_bitshuffle_3", dev.zarr.zarrjava.v2.DataType.INT32},
            new Object[]{"blosc", "zlib_shuffle_5", dev.zarr.zarrjava.v2.DataType.INT32},
            new Object[]{"blosc", "zstd_bitshuffle_9", dev.zarr.zarrjava.v2.DataType.INT32}
        );

        return Stream.concat(datatypeTests, bloscTests);
    }

    @ParameterizedTest
    @MethodSource("compressorAndDataTypeProviderV2")
    public void testReadV2(String compressor, String compressorParam, dev.zarr.zarrjava.v2.DataType dt) throws IOException, ZarrException, InterruptedException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testReadV2", compressor, compressorParam, dt.name());
        run_python_script("zarr_python_write_v2.py", compressor, compressorParam, dt.name().toLowerCase(), storeHandle.toPath().toString());

        dev.zarr.zarrjava.v2.Array array = dev.zarr.zarrjava.v2.Array.open(storeHandle);
        ucar.ma2.Array result = array.read();

        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        Assertions.assertEquals(dt, array.metadata().dataType);
        Assertions.assertArrayEquals(new int[]{2, 4, 8}, array.metadata().chunkShape());
        Assertions.assertEquals(42, array.metadata().attributes().get("answer"));

        assertIsTestdata(result, dt);
    }


    @ParameterizedTest
    @MethodSource("compressorAndDataTypeProviderV2")
    public void testWriteV2(String compressor, String compressorParam, dev.zarr.zarrjava.v2.DataType dt) throws Exception {
        Attributes attributes = new Attributes();
        attributes.put("test_key", "test_value");
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testCodecsWriteV2", compressor, compressorParam, dt.name());

        dev.zarr.zarrjava.v2.ArrayMetadataBuilder builder = dev.zarr.zarrjava.v2.Array.metadataBuilder()
            .withShape(16, 16, 16)
            .withDataType(dt)
            .withChunks(2, 4, 8)
            .withAttributes(attributes)
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
        writeArray.write(testdata(dt));

        //read in zarr-java
        dev.zarr.zarrjava.v2.Array readArray = dev.zarr.zarrjava.v2.Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        Assertions.assertEquals(dt, readArray.metadata().dataType);
        Assertions.assertArrayEquals(new int[]{2, 4, 8}, readArray.metadata().chunkShape());
        Assertions.assertEquals("test_value", readArray.metadata().attributes().get("test_key"));
        assertIsTestdata(result, dt);

        //read in zarr_python
        run_python_script("zarr_python_read_v2.py", compressor, compressorParam, dt.name().toLowerCase(), storeHandle.toPath().toString());
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
        long originalSize = Zstd.getFrameContentSize(compressed);
        byte[] decompressed = Zstd.decompress(compressed, (int) originalSize);
        Assertions.assertEquals(number, ByteBuffer.wrap(decompressed).getInt());

        //write compressed to file
        String compressedDataPath = TESTOUTPUT.resolve("compressed" + clevel + checksumFlag + ".bin").toString();
        try (FileOutputStream fos = new FileOutputStream(compressedDataPath)) {
            fos.write(compressed);
        }

        //decompress in python
        int exitCode = ZarrPythonTests.runCommand(
            "uv",
            "run",
            PYTHON_TEST_PATH.resolve("zstd_decompress.py").toString(),
            compressedDataPath,
            Integer.toString(number)
        );
        assert exitCode == 0;
    }

    @Test
    public void testGroupReadWriteV2() throws Exception {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("group_write");
        StoreHandle storeHandle2 = new FilesystemStore(TESTOUTPUT).resolve("group_read");
        Group group = Group.create(storeHandle, new Attributes(b -> b.set("attr", "value")));
        dev.zarr.zarrjava.v2.DataType dataType = dev.zarr.zarrjava.v2.DataType.INT32;
        dev.zarr.zarrjava.v2.Array array = group.createGroup("group").createArray("array", arrayMetadataBuilder -> arrayMetadataBuilder
                .withShape(16, 16, 16)
                .withDataType(dataType)
                .withChunks(2, 4, 8)
            );

        array.write(testdata(dataType));

        run_python_script("zarr_python_group.py", storeHandle.toPath().toString(), storeHandle2.toPath().toString(), "" + 2);

        Group group2 = Group.open(storeHandle2);
        Assertions.assertEquals("value", group2.metadata().attributes().get("attr"));
        Group subgroup = (Group) group2.get("group2");
        Assertions.assertNotNull(subgroup);
        dev.zarr.zarrjava.v2.Array array2 = (dev.zarr.zarrjava.v2.Array) subgroup.get("array2");
        Assertions.assertNotNull(array2);
        ucar.ma2.Array result = array2.read();
        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        assertIsTestdata(result, dataType);
    }

    @Test
    public void testGroupReadWriteV3() throws Exception {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("group_write");
        StoreHandle storeHandle2 = new FilesystemStore(TESTOUTPUT).resolve("group_read");
        dev.zarr.zarrjava.v3.Group group = dev.zarr.zarrjava.v3.Group.create(storeHandle, new Attributes(b -> b.set("attr", "value")));
        dev.zarr.zarrjava.v3.DataType dataType = DataType.INT32;
        dev.zarr.zarrjava.v3.Array array = group.createGroup("group").createArray("array", arrayMetadataBuilder -> arrayMetadataBuilder
                .withShape(16, 16, 16)
                .withDataType(dataType)
                .withChunkShape(2, 4, 8)
            );

        array.write(testdata(dataType));

        run_python_script("zarr_python_group.py", storeHandle.toPath().toString(), storeHandle2.toPath().toString(), "" + 3);

        dev.zarr.zarrjava.v3.Group group2 = dev.zarr.zarrjava.v3.Group.open(storeHandle2);
        Assertions.assertEquals("value", group2.metadata().attributes().get("attr"));
        dev.zarr.zarrjava.v3.Group subgroup = (dev.zarr.zarrjava.v3.Group) group2.get("group2");
        Assertions.assertNotNull(subgroup);
        dev.zarr.zarrjava.v3.Array array2 = (dev.zarr.zarrjava.v3.Array) subgroup.get("array2");
        Assertions.assertNotNull(array2);
        ucar.ma2.Array result = array2.read();
        Assertions.assertArrayEquals(new int[]{16, 16, 16}, result.getShape());
        assertIsTestdata(result, dataType);
    }
}
