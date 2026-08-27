package dev.zarr.zarrjava;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.Float16;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Tests for the {@code float16} data type.
 *
 * <p>The conversion cases are ground truth generated from {@code numpy.float16} (numpy 2.4.6);
 * hex literals are exact bit patterns so the assertions do not depend on decimal parsing.
 */
public class Float16Test extends ZarrTest {

    final static Path PYTHON_TEST_PATH = Paths.get("src/test/python-scripts/");

    /**
     * The subset of the binary16 range that is exactly representable, so round-trips are exact.
     * Mirrors VALUES in float16_interop.py.
     */
    static final float[] EDGE_CASES = new float[]{
            0.0f,
            -0.0f,
            1.0f,
            -2.0f,
            5.960464477539063E-8f,  // smallest positive subnormal, 2^-24
            6.097555160522461E-5f,  // largest subnormal
            6.103515625E-5f,        // smallest positive normal, 2^-14
            65504.0f,               // largest finite
            -65504.0f,
            Float.POSITIVE_INFINITY,
            Float.NEGATIVE_INFINITY,
            Float.NaN,
            0.333251953125f,        // 1/3 rounded to binary16
            -0.0999755859375f,      // 0.1 rounded to binary16, negated
    };

    static int runPython(String... args) throws IOException, InterruptedException {
        String[] command = new String[args.length + 2];
        command[0] = "uv";
        command[1] = "run";
        System.arraycopy(args, 0, command, 2, args.length);

        ProcessBuilder pb = new ProcessBuilder(command);
        Process process = pb.start();
        try (BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = out.readLine()) != null) {
                System.out.println(line);
            }
        }
        try (BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
            String line;
            while ((line = err.readLine()) != null) {
                System.err.println(line);
            }
        }
        return process.waitFor();
    }

    // ---------------------------------------------------------------- bit conversions

    /**
     * Widening binary16 to float. Ground truth: numpy float32 bit patterns.
     */
    @ParameterizedTest
    @CsvSource({
            "0x0000, 0x00000000",  // +0.0
            "0x8000, 0x80000000",  // -0.0
            "0x0001, 0x33800000",  // smallest subnormal, 2^-24
            "0x8001, 0xB3800000",  // negative smallest subnormal
            "0x0002, 0x34000000",  // subnormal needing 9 normalization shifts
            "0x03FF, 0x387FC000",  // largest subnormal
            "0x0400, 0x38800000",  // smallest normal, 2^-14
            "0x3C00, 0x3F800000",  // 1.0
            "0xBC00, 0xBF800000",  // -1.0
            "0xC000, 0xC0000000",  // -2.0
            "0x7BFF, 0x477FE000",  // 65504.0, largest finite
            "0xFBFF, 0xC77FE000",  // -65504.0
            "0x7C00, 0x7F800000",  // +Infinity
            "0xFC00, 0xFF800000",  // -Infinity
            "0x7E00, 0x7FC00000",  // NaN, payload preserved
            "0x3555, 0x3EAAA000",  // 1/3 rounded
    })
    public void testHalfBitsToFloat(String halfHex, String floatHex) {
        short halfBits = (short) Integer.parseInt(halfHex.substring(2), 16);
        int expectedBits = (int) Long.parseLong(floatHex.substring(2), 16);

        float actual = Float16.halfBitsToFloat(halfBits);

        Assertions.assertEquals(
                String.format("%08X", expectedBits),
                String.format("%08X", Float.floatToRawIntBits(actual)),
                "widening " + halfHex + " gave " + actual);
    }

    /**
     * Narrowing float to binary16, including round-half-to-even ties and overflow. Ground truth:
     * numpy float16 bit patterns.
     */
    @ParameterizedTest
    @CsvSource({
            "1.0, 0x3C00",
            "-2.0, 0xC000",
            "0.0, 0x0000",
            "-0.0, 0x8000",             // signed zero survives
            "65504.0, 0x7BFF",          // largest finite
            "65519.0, 0x7BFF",          // just below the overflow midpoint
            "65520.0, 0x7C00",          // exactly the midpoint: ties-to-even overflows to Infinity
            "65536.0, 0x7C00",
            "1.0E8, 0x7C00",            // far overflow
            "-1.0E8, 0xFC00",
            "5.9604645E-8, 0x0001",     // smallest subnormal, exact
            "3.0E-8, 0x0001",           // rounds up to the smallest subnormal
            "2.9802322E-8, 0x0000",     // exactly 2^-25: ties-to-even rounds to zero
            "1.5E-8, 0x0000",           // underflows
            "6.1035156E-5, 0x0400",     // smallest normal
            "6.0E-5, 0x03EF",           // falls into the subnormal range
            "0.1, 0x2E66",
            "2048.0, 0x6800",           // last exactly-representable integer
            "2049.0, 0x6800",           // tie rounds down to even
            "2050.0, 0x6801",
            "2051.0, 0x6802",           // tie rounds up to even
            "1.0009765625, 0x3C01",     // one ulp above 1.0
            "1.00048828125, 0x3C00",    // half an ulp: ties-to-even rounds down
    })
    public void testFloatToHalfBits(float value, String expectedHex) {
        short expected = (short) Integer.parseInt(expectedHex.substring(2), 16);

        short actual = Float16.floatToHalfBits(value);

        Assertions.assertEquals(
                String.format("%04X", expected & 0xFFFF),
                String.format("%04X", actual & 0xFFFF),
                "narrowing " + value);
    }

    @Test
    public void testNaNNarrowsToNaN() {
        // A NaN whose payload lives entirely in the low mantissa bits would round to a zero
        // payload, turning it into an Infinity. It must stay a NaN.
        float sneakyNaN = Float.intBitsToFloat(0x7F800001);
        Assertions.assertTrue(Float.isNaN(sneakyNaN), "precondition: input is NaN");

        short halfBits = Float16.floatToHalfBits(sneakyNaN);

        Assertions.assertTrue(Float.isNaN(Float16.halfBitsToFloat(halfBits)),
                "NaN must not narrow to Infinity, got bits 0x" + String.format("%04X", halfBits & 0xFFFF));
    }

    @Test
    public void testRoundTripIsIdentityForRepresentableValues() {
        for (float value : EDGE_CASES) {
            float actual = Float16.halfBitsToFloat(Float16.floatToHalfBits(value));
            if (Float.isNaN(value)) {
                Assertions.assertTrue(Float.isNaN(actual), "NaN round-trip");
            } else {
                Assertions.assertEquals(
                        Float.floatToRawIntBits(value), Float.floatToRawIntBits(actual),
                        "round-trip of " + value + " gave " + actual);
            }
        }
    }

    @Test
    public void testEveryHalfBitPatternRoundTrips() {
        // Exhaustive over all 65536 patterns: widening then narrowing must be the identity on
        // bits, except that NaN payloads are only required to stay NaN.
        for (int bits = 0; bits <= 0xFFFF; bits++) {
            short halfBits = (short) bits;
            float widened = Float16.halfBitsToFloat(halfBits);
            short narrowed = Float16.floatToHalfBits(widened);
            if (Float.isNaN(widened)) {
                Assertions.assertTrue(Float.isNaN(Float16.halfBitsToFloat(narrowed)),
                        "NaN pattern 0x" + String.format("%04X", bits) + " stopped being NaN");
            } else {
                Assertions.assertEquals(bits, narrowed & 0xFFFF,
                        "pattern 0x" + String.format("%04X", bits) + " did not round-trip");
            }
        }
    }

    // ---------------------------------------------------------------- metadata

    @Test
    public void testMetadataParsesFloat16() throws Exception {
        String json = "{\"zarr_format\":3,\"node_type\":\"array\",\"shape\":[4],"
                + "\"data_type\":\"float16\","
                + "\"chunk_grid\":{\"name\":\"regular\",\"configuration\":{\"chunk_shape\":[2]}},"
                + "\"chunk_key_encoding\":{\"name\":\"default\"},\"fill_value\":0,"
                + "\"codecs\":[{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}]}";

        dev.zarr.zarrjava.v3.ArrayMetadata metadata = dev.zarr.zarrjava.v3.Node.makeObjectMapper()
                .readValue(json, dev.zarr.zarrjava.v3.ArrayMetadata.class);

        Assertions.assertEquals(DataType.FLOAT16, metadata.dataType);
        Assertions.assertEquals(2, metadata.dataType.getByteCount());
        Assertions.assertEquals(ucar.ma2.DataType.FLOAT, metadata.dataType.getMA2DataType());
        Assertions.assertTrue(metadata.dataType.isHalfPrecisionFloat());
    }

    @Test
    public void testDataTypeSerializesToFloat16() throws Exception {
        String json = dev.zarr.zarrjava.v3.Node.makeObjectMapper().writeValueAsString(DataType.FLOAT16);
        Assertions.assertEquals("\"float16\"", json);
    }

    /**
     * Fill values, in every form the spec allows for a float. The hex and binary forms carry a
     * binary16 bit pattern, not a truncated float32.
     */
    @ParameterizedTest
    @CsvSource({
            "0, 0.0",
            "1, 1.0",
            "-2.5, -2.5",
            "'NaN', NaN",
            "'+Infinity', Infinity",
            "'-Infinity', -Infinity",
            "'0x003C', 1.0",              // 0x3C00 little-endian
            "'0x00C0', -2.0",             // 0xC000 little-endian
            "'0b0000000000111100', 1.0",  // same bits, binary form
    })
    public void testFillValueForms(String fillValueJson, String expected) throws Exception {
        String quoted = fillValueJson.startsWith("0x") || fillValueJson.startsWith("0b")
                || fillValueJson.equals("NaN") || fillValueJson.endsWith("Infinity")
                ? "\"" + fillValueJson + "\"" : fillValueJson;
        String json = "{\"zarr_format\":3,\"node_type\":\"array\",\"shape\":[4],"
                + "\"data_type\":\"float16\","
                + "\"chunk_grid\":{\"name\":\"regular\",\"configuration\":{\"chunk_shape\":[2]}},"
                + "\"chunk_key_encoding\":{\"name\":\"default\"},"
                + "\"fill_value\":" + quoted + ","
                + "\"codecs\":[{\"name\":\"bytes\",\"configuration\":{\"endian\":\"little\"}}]}";

        dev.zarr.zarrjava.v3.ArrayMetadata metadata = dev.zarr.zarrjava.v3.Node.makeObjectMapper()
                .readValue(json, dev.zarr.zarrjava.v3.ArrayMetadata.class);

        Assertions.assertEquals(Float.class, metadata.parsedFillValue.getClass(),
                "float16 fill values are parsed to Float, matching the in-memory type");
        Assertions.assertEquals(Float.parseFloat(expected), (Float) metadata.parsedFillValue, 1e-9f);
    }

    @Test
    public void testUnwrittenChunksReadAsFillValue() throws Exception {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("float16_fill");
        Array array = Array.create(storeHandle, Array.metadataBuilder()
                .withShape(4)
                .withDataType(DataType.FLOAT16)
                .withChunkShape(2)
                .withFillValue(-2.5)
                .withCodecs(c -> c.withBytes(BytesCodec.Endian.LITTLE))
                .build());

        ucar.ma2.Array result = array.read();

        Assertions.assertEquals(ucar.ma2.DataType.FLOAT, result.getDataType());
        for (int i = 0; i < 4; i++) {
            Assertions.assertEquals(-2.5f, result.getFloat(i), 0.0f, "index " + i);
        }
    }

    // ---------------------------------------------------------------- zarr-java round-trip

    @ParameterizedTest
    @EnumSource(BytesCodec.Endian.class)
    public void testRoundTripBothEndiannesses(BytesCodec.Endian endian) throws Exception {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT)
                .resolve("float16_roundtrip", endian.name());

        Array array = Array.create(storeHandle, Array.metadataBuilder()
                .withShape(EDGE_CASES.length)
                .withDataType(DataType.FLOAT16)
                .withChunkShape(5)
                .withFillValue(0)
                .withCodecs(c -> c.withBytes(endian))
                .build());

        array.write(ucar.ma2.Array.factory(
                ucar.ma2.DataType.FLOAT, new int[]{EDGE_CASES.length}, EDGE_CASES.clone()));

        ucar.ma2.Array result = Array.open(storeHandle).read();

        assertEdgeCases(result);
    }

    @Test
    public void testEncodedChunkIsTwoBytesPerElement() throws Exception {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("float16_width");
        Array array = Array.create(storeHandle, Array.metadataBuilder()
                .withShape(8)
                .withDataType(DataType.FLOAT16)
                .withChunkShape(8)
                .withFillValue(0)
                .withCodecs(c -> c.withBytes(BytesCodec.Endian.LITTLE))
                .build());

        array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.FLOAT, new int[]{8},
                new float[]{1, 2, 3, 4, 5, 6, 7, 8}));

        long chunkSize = storeHandle.resolve("c", "0").getSize();
        Assertions.assertEquals(16, chunkSize,
                "8 float16 elements must encode to 16 bytes, not 32");
    }

    @Test
    public void testEndiannessActuallySwapsBytes() throws Exception {
        // 1.0 is 0x3C00, whose bytes differ between orders, so a byte-level check is meaningful.
        long[] sizes = new long[2];
        byte[][] firstBytes = new byte[2][];
        BytesCodec.Endian[] orders = {BytesCodec.Endian.LITTLE, BytesCodec.Endian.BIG};

        for (int i = 0; i < orders.length; i++) {
            StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT)
                    .resolve("float16_endian", orders[i].name());
            final BytesCodec.Endian endian = orders[i];
            Array array = Array.create(storeHandle, Array.metadataBuilder()
                    .withShape(1)
                    .withDataType(DataType.FLOAT16)
                    .withChunkShape(1)
                    .withFillValue(0)
                    .withCodecs(c -> c.withBytes(endian))
                    .build());
            array.write(ucar.ma2.Array.factory(ucar.ma2.DataType.FLOAT, new int[]{1},
                    new float[]{1.0f}));

            byte[] bytes = dev.zarr.zarrjava.utils.Utils.toArray(
                    storeHandle.resolve("c", "0").readNonNull());
            sizes[i] = bytes.length;
            firstBytes[i] = bytes;
        }

        Assertions.assertArrayEquals(new byte[]{0x00, 0x3C}, firstBytes[0], "little-endian 1.0");
        Assertions.assertArrayEquals(new byte[]{0x3C, 0x00}, firstBytes[1], "big-endian 1.0");
        Assertions.assertEquals(2, sizes[0]);
    }

    @ParameterizedTest
    @ValueSource(strings = {"blosc", "zstd", "gzip", "sharding", "crc32c"})
    public void testRoundTripThroughCompressingCodecs(String codec) throws Exception {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("float16_codec", codec);

        Array array = Array.create(storeHandle, Array.metadataBuilder()
                .withShape(EDGE_CASES.length)
                .withDataType(DataType.FLOAT16)
                .withChunkShape(EDGE_CASES.length)
                .withFillValue(0)
                .withCodecs(c -> {
                    switch (codec) {
                        case "blosc":
                            // typesize must follow the encoded width, 2 bytes, not the in-memory 4.
                            return c.withBlosc("zstd", "shuffle", 5);
                        case "zstd":
                            return c.withZstd(3);
                        case "gzip":
                            return c.withGzip(5);
                        case "sharding":
                            return c.withSharding(new int[]{7}, c1 -> c1.withBytes("LITTLE"));
                        case "crc32c":
                            return c.withBytes(BytesCodec.Endian.LITTLE).withCrc32c();
                        default:
                            throw new IllegalArgumentException(codec);
                    }
                })
                .build());

        array.write(ucar.ma2.Array.factory(
                ucar.ma2.DataType.FLOAT, new int[]{EDGE_CASES.length}, EDGE_CASES.clone()));

        assertEdgeCases(Array.open(storeHandle).read());
    }

    @Test
    public void testBloscTypesizeIsEncodedWidth() {
        Assertions.assertEquals(2, DataType.FLOAT16.getByteCount(),
                "blosc typesize is derived from getByteCount, which must be the encoded width");
    }

    // ---------------------------------------------------------------- v2

    @Test
    public void testV2RoundTrip() throws Exception {
        for (dev.zarr.zarrjava.v2.DataType dt : new dev.zarr.zarrjava.v2.DataType[]{
                dev.zarr.zarrjava.v2.DataType.FLOAT16, dev.zarr.zarrjava.v2.DataType.FLOAT16_BE}) {
            StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT)
                    .resolve("float16_v2", dt.name());

            dev.zarr.zarrjava.v2.Array array = dev.zarr.zarrjava.v2.Array.create(
                    storeHandle,
                    dev.zarr.zarrjava.v2.Array.metadataBuilder()
                            .withShape(EDGE_CASES.length)
                            .withChunks(5)
                            .withDataType(dt)
                            .withFillValue(0)
                            .build());

            array.write(ucar.ma2.Array.factory(
                    ucar.ma2.DataType.FLOAT, new int[]{EDGE_CASES.length}, EDGE_CASES.clone()));

            Assertions.assertEquals(2, dt.getByteCount(), dt + " byte count");
            assertEdgeCases(dev.zarr.zarrjava.v2.Array.open(storeHandle).read());
        }
    }

    @Test
    public void testV2DtypeStrings() {
        Assertions.assertEquals("<f2", dev.zarr.zarrjava.v2.DataType.FLOAT16.getValue());
        Assertions.assertEquals(">f2", dev.zarr.zarrjava.v2.DataType.FLOAT16_BE.getValue());
    }

    // ---------------------------------------------------------------- zarr-python interop

    @ParameterizedTest
    @CsvSource({"3, LITTLE", "3, BIG", "2, LITTLE", "2, BIG"})
    public void testPythonWritesJavaReads(int zarrFormat, String endian) throws Exception {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT)
                .resolve("float16_py2java", "v" + zarrFormat, endian);

        int exit = runPython(PYTHON_TEST_PATH.resolve("float16_interop.py").toString(),
                "write", storeHandle.toPath().toString(), String.valueOf(zarrFormat), endian);
        Assertions.assertEquals(0, exit, "zarr-python writer failed");

        ucar.ma2.Array result = zarrFormat == 3
                ? Array.open(storeHandle).read()
                : dev.zarr.zarrjava.v2.Array.open(storeHandle).read();

        assertEdgeCases(result);
    }

    @ParameterizedTest
    @CsvSource({"3, LITTLE", "3, BIG", "2, LITTLE", "2, BIG"})
    public void testJavaWritesPythonReads(int zarrFormat, String endian) throws Exception {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT)
                .resolve("float16_java2py", "v" + zarrFormat, endian);

        if (zarrFormat == 3) {
            Array.create(storeHandle, Array.metadataBuilder()
                            .withShape(EDGE_CASES.length)
                            .withDataType(DataType.FLOAT16)
                            .withChunkShape(5)
                            .withFillValue(0)
                            .withCodecs(c -> c.withBytes(BytesCodec.Endian.valueOf(endian)))
                            .build())
                    .write(ucar.ma2.Array.factory(ucar.ma2.DataType.FLOAT,
                            new int[]{EDGE_CASES.length}, EDGE_CASES.clone()));
        } else {
            dev.zarr.zarrjava.v2.DataType dt = endian.equals("BIG")
                    ? dev.zarr.zarrjava.v2.DataType.FLOAT16_BE
                    : dev.zarr.zarrjava.v2.DataType.FLOAT16;
            dev.zarr.zarrjava.v2.Array.create(storeHandle,
                            dev.zarr.zarrjava.v2.Array.metadataBuilder()
                                    .withShape(EDGE_CASES.length)
                                    .withChunks(5)
                                    .withDataType(dt)
                                    .withFillValue(0)
                                    .build())
                    .write(ucar.ma2.Array.factory(ucar.ma2.DataType.FLOAT,
                            new int[]{EDGE_CASES.length}, EDGE_CASES.clone()));
        }

        int exit = runPython(PYTHON_TEST_PATH.resolve("float16_interop.py").toString(),
                "verify", storeHandle.toPath().toString(), String.valueOf(zarrFormat));
        Assertions.assertEquals(0, exit, "zarr-python could not read what zarr-java wrote");
    }

    // ---------------------------------------------------------------- helper

    private static void assertEdgeCases(ucar.ma2.Array result) {
        Assertions.assertEquals(EDGE_CASES.length, result.getSize(), "element count");
        for (int i = 0; i < EDGE_CASES.length; i++) {
            float expected = EDGE_CASES[i];
            float actual = result.getFloat(i);
            if (Float.isNaN(expected)) {
                Assertions.assertTrue(Float.isNaN(actual), "index " + i + " should be NaN");
            } else {
                // Raw bits, so that +0.0 and -0.0 are told apart.
                Assertions.assertEquals(
                        Float.floatToRawIntBits(expected), Float.floatToRawIntBits(actual),
                        "index " + i + ": expected " + expected + " got " + actual
                                + " (all: " + Arrays.toString(
                                (float[]) result.get1DJavaArray(ucar.ma2.DataType.FLOAT)) + ")");
            }
        }
    }
}
