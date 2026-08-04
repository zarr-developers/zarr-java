package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.HttpStore;
import dev.zarr.zarrjava.store.MemoryStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.v3.*;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import dev.zarr.zarrjava.v3.codec.core.BloscCodec;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;
import dev.zarr.zarrjava.v3.codec.core.JpegCodec;
import dev.zarr.zarrjava.v3.codec.core.ShardingIndexedCodec;
import dev.zarr.zarrjava.v3.codec.core.TransposeCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import ucar.ma2.MAMath;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dev.zarr.zarrjava.core.ArrayMetadata.parseFillValue;
import static dev.zarr.zarrjava.core.Node.ZARR_JSON;
import static dev.zarr.zarrjava.utils.Utils.toLongArray;
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

    static Stream<int[]> invalidChunkSizes() {
        return Stream.of(
                new int[]{1},
                new int[]{1, 1, 1}
        );
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

    static Stream<int[]> invalidTransposeOrder() {
        return Stream.of(
                new int[]{1, 0, 0},
                new int[]{1, 2, 3},
                new int[]{1, 2, 3, 0},
                new int[]{1, 2}
        );
    }

    static Stream<Function<CodecBuilder, CodecBuilder>> codecBuilders() {
        return Stream.of(
                CodecBuilder::withBlosc,
                c -> c.withTranspose(new int[]{1, 0}),
                CodecBuilder::withBytes,
                CodecBuilder::withGzip,
                CodecBuilder::withZstd,
                c -> c.withSharding(new int[]{2, 2}),
                CodecBuilder::withCrc32c
        );
    }

    static Stream<Function<ArrayMetadataBuilder, ArrayMetadataBuilder>> chunkKeyEncodingsAndCodecs() {
        Stream<Function<ArrayMetadataBuilder, ArrayMetadataBuilder>> builders = Stream.of(
                ArrayMetadataBuilder::withDefaultChunkKeyEncoding,
                ArrayMetadataBuilder::withV2ChunkKeyEncoding
        );

        return Stream.concat(builders, codecBuilders().map(codecFunc -> b -> b.withCodecs(codecFunc)));
    }

    static Stream<Arguments> unalignedArrayAccessProvider() {
        Stream.Builder<Arguments> builder = Stream.builder();
        builder.add(Arguments.of(52, 17, 32));
        builder.add(Arguments.of(71, 11, 12));
        builder.add(Arguments.of(52, 17, 17));
        builder.add(Arguments.of(50, 3, 7));
        builder.add(Arguments.of(50, 3, 22));
        builder.add(Arguments.of(13, 31, 21));
        return builder.build();
    }

    static Stream<Arguments> dataTypeAndEndianProvider() {
        return Stream.of(
                Arguments.of(DataType.INT16, BytesCodec.Endian.LITTLE),
                Arguments.of(DataType.INT16, BytesCodec.Endian.BIG),
                Arguments.of(DataType.UINT16, BytesCodec.Endian.LITTLE),
                Arguments.of(DataType.UINT16, BytesCodec.Endian.BIG),
                Arguments.of(DataType.INT32, BytesCodec.Endian.LITTLE),
                Arguments.of(DataType.INT32, BytesCodec.Endian.BIG),
                Arguments.of(DataType.UINT32, BytesCodec.Endian.LITTLE),
                Arguments.of(DataType.UINT32, BytesCodec.Endian.BIG),
                Arguments.of(DataType.FLOAT32, BytesCodec.Endian.LITTLE),
                Arguments.of(DataType.FLOAT32, BytesCodec.Endian.BIG),
                Arguments.of(DataType.FLOAT64, BytesCodec.Endian.LITTLE),
                Arguments.of(DataType.FLOAT64, BytesCodec.Endian.BIG)
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
    public void testZstdCodecReadWrite(int level, boolean checksum) throws ZarrException, IOException {
        int[] testData = new int[16 * 16 * 16];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testZstdCodecReadWrite", "checksum_" + checksum, "level_" + level);
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 16)
                .withDataType(DataType.UINT32)
                .withChunkShape(2, 4, 8)
                .withFillValue(0)
                .withCodecs(c -> c.withZstd(level, checksum));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{16, 16, 16}, testData));

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
    }

    /** Maximum absolute difference between two uint8 buffers (compared as unsigned). */
    private static int maxAbsDiff(byte[] expected, byte[] actual) {
        Assertions.assertEquals(expected.length, actual.length);
        int max = 0;
        for (int i = 0; i < expected.length; i++) {
            int diff = Math.abs((expected[i] & 0xff) - (actual[i] & 0xff));
            if (diff > max) {
                max = diff;
            }
        }
        return max;
    }

    @ParameterizedTest
    @CsvSource({"75", "90", "100"})
    public void testJpegCodecGrayscaleReadWrite(int quality) throws ZarrException, IOException {
        // A 2D (H, W) chunk is grayscale.
        int[] shape = {16, 16};
        int n = shape[0] * shape[1];
        byte[] testData = new byte[n];
        // Smooth global ramp so the lossy JPEG stays close to the original.
        for (int i = 0; i < n; i++) {
            testData[i] = (byte) (255 * i / n);
        }

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegGray", "q" + quality);
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(quality));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, shape, testData));

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        byte[] roundTripped = (byte[]) result.copyTo1DJavaArray();
        Assertions.assertTrue(maxAbsDiff(testData, roundTripped) <= 4,
                "grayscale round-trip differs too much at quality " + quality);
    }

    @Test
    public void testJpegCodecGrayscaleWithChannelAxisReadWrite() throws ZarrException, IOException {
        // A 3D (H, W, 1) chunk is grayscale too (so data can be sharded over a size-1 channel axis).
        int[] shape = {16, 16, 1};
        int n = shape[0] * shape[1] * shape[2];
        byte[] testData = new byte[n];
        for (int i = 0; i < n; i++) {
            testData[i] = (byte) (255 * i / n);
        }

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegGrayChannelAxis");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 1)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16, 1)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(100));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, shape, testData));

        ucar.ma2.Array result = Array.open(storeHandle).read();
        byte[] roundTripped = (byte[]) result.copyTo1DJavaArray();
        Assertions.assertTrue(maxAbsDiff(testData, roundTripped) <= 4);
    }

    private static byte[] rgbRampData(int numPixels) {
        byte[] testData = new byte[numPixels * 3];
        // Smooth per-channel ramps so the lossy JPEG stays close to the original.
        for (int p = 0; p < numPixels; p++) {
            testData[p * 3] = (byte) (255 * p / numPixels);
            testData[p * 3 + 1] = (byte) (255 - 255 * p / numPixels);
            testData[p * 3 + 2] = (byte) (128 * p / numPixels);
        }
        return testData;
    }

    @ParameterizedTest
    @CsvSource({"90", "100"})
    public void testJpegCodecYCbCrReadWrite(int quality) throws ZarrException, IOException {
        int[] shape = {16, 16, 3};
        byte[] testData = rgbRampData(shape[0] * shape[1]);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegYCbCr", "q" + quality);
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 3)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16, 3)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(quality, "ycbcr"));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, shape, testData));

        ucar.ma2.Array result = Array.open(storeHandle).read();
        byte[] roundTripped = (byte[]) result.copyTo1DJavaArray();
        // RGB goes through the YCbCr transform and (by default) 4:2:0 chroma subsampling, so a
        // looser tolerance than grayscale is expected.
        Assertions.assertTrue(maxAbsDiff(testData, roundTripped) <= 24,
                "ycbcr round-trip differs too much at quality " + quality);
    }

    @Test
    public void testJpegCodecRgbColorSpaceReadWrite() throws ZarrException, IOException {
        // encoded_color_space "rgb" stores the three components without any color transform, so at
        // quality 100 the round-trip is near-lossless (only DCT quantization loss).
        int[] shape = {16, 16, 3};
        byte[] testData = rgbRampData(shape[0] * shape[1]);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegRgbColorSpace");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 3)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16, 3)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(100, "rgb"));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, shape, testData));

        ucar.ma2.Array result = Array.open(storeHandle).read();
        byte[] roundTripped = (byte[]) result.copyTo1DJavaArray();
        Assertions.assertTrue(maxAbsDiff(testData, roundTripped) <= 4,
                "rgb (no color transform) round-trip differs too much");
    }

    @Test
    public void testJpegCodecLargeChunkReadWrite() throws ZarrException, IOException {
        // A chunk with more than 65535 pixels is stored as-is (no width x height factorization).
        int[] shape = {300, 300};
        int n = shape[0] * shape[1];
        byte[] testData = new byte[n];
        for (int i = 0; i < n; i++) {
            testData[i] = (byte) (255 * i / n);
        }

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegLargeChunk");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(300, 300)
                .withDataType(DataType.UINT8)
                .withChunkShape(300, 300)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(100));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, shape, testData));

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        byte[] roundTripped = (byte[]) result.copyTo1DJavaArray();
        Assertions.assertTrue(maxAbsDiff(testData, roundTripped) <= 4);
    }

    @Test
    public void testJpegCodecRejectsNonUint8() throws ZarrException, IOException {
        int[] testData = new int[16 * 16];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegRejectsNonUint8");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16)
                .withDataType(DataType.UINT32)
                .withChunkShape(16, 16)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg());
        Array writeArray = Array.create(storeHandle, builder.build());
        // The parallel writer wraps the codec's ZarrException in a RuntimeException.
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{16, 16}, testData)));
        Assertions.assertTrue(ex.getMessage().contains("uint8"), ex.getMessage());
    }

    @Test
    public void testJpegCodecRejectsUnsupportedShape() throws ZarrException, IOException {
        // (H, W, 2) has neither 1 nor 3 in the channel axis and must be rejected.
        byte[] testData = new byte[16 * 16 * 2];
        Arrays.fill(testData, (byte) 1); // non-fill data so the chunk is actually encoded

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegRejectsShape");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 2)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16, 2)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg());
        Array writeArray = Array.create(storeHandle, builder.build());
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{16, 16, 2}, testData)));
        Assertions.assertTrue(ex.getMessage().contains("chunk shapes"), ex.getMessage());
    }

    @Test
    public void testJpegCodecRejectsColorWithoutColorSpace() throws ZarrException, IOException {
        // 3-component data requires encoded_color_space.
        byte[] testData = new byte[16 * 16 * 3];
        Arrays.fill(testData, (byte) 1); // non-fill data so the chunk is actually encoded

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegRejectsNoColorSpace");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 3)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16, 3)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(90));
        Array writeArray = Array.create(storeHandle, builder.build());
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{16, 16, 3}, testData)));
        Assertions.assertTrue(ex.getMessage().contains("encoded_color_space"), ex.getMessage());
    }

    @Test
    public void testJpegCodecRejectsColorSpaceForGrayscale() throws ZarrException, IOException {
        // encoded_color_space must not be set for grayscale data.
        byte[] testData = new byte[16 * 16];
        Arrays.fill(testData, (byte) 1); // non-fill data so the chunk is actually encoded

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegRejectsGrayColorSpace");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(90, "ycbcr"));
        Array writeArray = Array.create(storeHandle, builder.build());
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{16, 16}, testData)));
        Assertions.assertTrue(ex.getMessage().contains("encoded_color_space"), ex.getMessage());
    }

    @Test
    public void testJpegCodecYCbCr440ReadWrite() throws ZarrException, IOException {
        // The 4:4:0 scheme ([[1, 2], [1, 1], [1, 1]]) subsamples chroma vertically only.
        int[] shape = {16, 16, 3};
        byte[] testData = rgbRampData(shape[0] * shape[1]);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegYCbCr440");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 3)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16, 3)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(100, "ycbcr", new int[][]{{1, 2}, {1, 1}, {1, 1}}));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, shape, testData));

        ucar.ma2.Array result = Array.open(storeHandle).read();
        byte[] roundTripped = (byte[]) result.copyTo1DJavaArray();
        Assertions.assertTrue(maxAbsDiff(testData, roundTripped) <= 24);
    }

    @Test
    public void testJpegCodecRejectsRgbWithSubsampling() {
        // subsampling other than [[1,1],[1,1],[1,1]] is invalid with encoded_color_space "rgb".
        ZarrException ex = assertThrows(ZarrException.class,
                () -> new JpegCodec.Configuration(90, "rgb", new int[][]{{2, 2}, {1, 1}, {1, 1}}));
        Assertions.assertTrue(ex.getMessage().contains("rgb"), ex.getMessage());
    }

    @Test
    public void testJpegCodecRejectsSubsampledChroma() {
        // The chroma components must have sampling factor [1, 1].
        ZarrException ex = assertThrows(ZarrException.class,
                () -> new JpegCodec.Configuration(90, "ycbcr", new int[][]{{2, 2}, {2, 2}, {1, 1}}));
        Assertions.assertTrue(ex.getMessage().contains("chroma"), ex.getMessage());
    }

    @Test
    public void testJpegCodecRejectsSubsamplingLengthMismatch() throws ZarrException, IOException {
        // A subsampling array whose length does not match the component count is rejected.
        byte[] testData = new byte[16 * 16 * 3];
        Arrays.fill(testData, (byte) 1);

        StoreHandle storeHandle =
                new FilesystemStore(TESTOUTPUT).resolve("testJpegRejectsSubsamplingLength");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 3)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16, 3)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(90, "ycbcr", new int[][]{{2, 2}, {1, 1}}));
        Array writeArray = Array.create(storeHandle, builder.build());
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, new int[]{16, 16, 3}, testData)));
        Assertions.assertTrue(ex.getMessage().contains("one entry per component"), ex.getMessage());
    }

    @Test
    public void testJpegCodecMetadataRoundTrip() throws ZarrException, IOException {
        int[] shape = {16, 16, 3};
        byte[] testData = rgbRampData(shape[0] * shape[1]);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegMetadata");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 3)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16, 3)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(75, "ycbcr", new int[][]{{2, 1}, {1, 1}, {1, 1}}));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, shape, testData));

        String zarrJson = new String(Files.readAllBytes(
                Paths.get("testoutput", "testJpegMetadata", ZARR_JSON)));
        Assertions.assertTrue(zarrJson.contains("\"name\" : \"jpeg\""), zarrJson);
        Assertions.assertTrue(zarrJson.contains("\"quality\" : 75"), zarrJson);
        Assertions.assertTrue(zarrJson.contains("\"encoded_color_space\" : \"ycbcr\""), zarrJson);
        Assertions.assertTrue(zarrJson.replaceAll("\\s+", "").contains("\"subsampling\":[[2,1],[1,1],[1,1]]"),
                zarrJson);

        // Re-opening exercises deserialization of the jpeg codec from metadata.
        Assertions.assertNotNull(Array.open(storeHandle).read());
    }

    @Test
    public void testShardingWithZstdCodecReadWrite() throws ZarrException, IOException {
        int[] testData = new int[16 * 16 * 16];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testShardingWithZstdCodecReadWrite");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 16)
                .withDataType(DataType.UINT32)
                .withChunkShape(8, 8, 8)
                .withFillValue(0)
                .withCodecs(c -> c.withSharding(new int[]{2, 4, 8}, c1 -> c1.withZstd()));
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

        ucar.ma2.Array outArray = array.read(new long[]{0, 3073, 3073, 513}, new long[]{1, 64, 64, 64});
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
        long[] readShape = new long[]{1, 1, 1024, 1024};
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
                new long[]{1, 64, 64, 64} // shape
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
        ucar.ma2.Array output = array.read(new long[]{0, 0, 0, 0}, new long[]{1, 1, 2, 2});
        assert MultiArrayUtils.allValuesEqual(data, output);
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "16-16-4"})
    public void testReadL4Sample(String mag) throws IOException, ZarrException {
        StoreHandle httpStoreHandle = new HttpStore("https://static.webknossos.org/data/zarr_v3/").resolve("l4_sample", "color", mag);
        StoreHandle localStoreHandle = new FilesystemStore(TESTDATA).resolve("l4_sample", "color", mag);

        Array httpArray = Array.open(httpStoreHandle);
        Array localArray = Array.open(localStoreHandle);

        Assertions.assertArrayEquals(httpArray.metadata().shape, localArray.metadata().shape);
        Assertions.assertArrayEquals(httpArray.metadata().chunkShape(), localArray.metadata().chunkShape());

        ucar.ma2.Array httpData1 = httpArray.read(new long[]{0, 0, 0, 0}, new long[]{1, 64, 64, 64});
        ucar.ma2.Array localData1 = localArray.read(new long[]{0, 0, 0, 0}, new long[]{1, 64, 64, 64});

        assert MultiArrayUtils.allValuesEqual(httpData1, localData1);

        //offset to where l4_sample contains non-zero values
        long[] offset = new long[4];
        long[] originalOffset = new long[]{0, 3073, 3073, 513};
        long[] originalShape = new long[]{1, 4096, 4096, 2048};
        long[] arrayShape = httpArray.metadata().shape;
        for (int i = 0; i < 4; i++) {
            offset[i] = originalOffset[i] / (originalShape[i] / arrayShape[i]);
        }

        ucar.ma2.Array httpData2 = httpArray.read(offset, new long[]{1, 64, 64, 64});
        ucar.ma2.Array localData2 = localArray.read(offset, new long[]{1, 64, 64, 64});

        assert MultiArrayUtils.allValuesEqual(httpData2, localData2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testParallel(boolean useParallel) throws IOException, ZarrException {
        int[] testData = new int[512 * 512 * 512];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testParallelRead", useParallel ? "parallel" : "serial");
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

        Array[] arrays = group.list().filter(n -> n instanceof Array).toArray(Array[]::new);
        Assertions.assertEquals(1, arrays.length);
        Assertions.assertArrayEquals(new int[]{5, 5}, arrays[0].metadata().chunkShape());
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
    public void testCodecWithoutConfiguration() throws ZarrException, IOException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testCodecWithoutConfigurationV3");
        Array array = Array.create(storeHandle, Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunkShape(5, 5)
                .withCodecs(CodecBuilder::withBytes)
                .build()
        );
        Assertions.assertTrue(storeHandle.resolve(ZARR_JSON).exists());
        Codec bytesCodec = array.metadata().codecs[0];
        Assertions.assertInstanceOf(BytesCodec.class, bytesCodec);
        Assertions.assertNull(((BytesCodec) bytesCodec).configuration);
    }

    @ParameterizedTest
    @MethodSource("chunkKeyEncodingsAndCodecs")
    public void testZarrJsonFormat(Function<ArrayMetadataBuilder, ArrayMetadataBuilder> chunkKeyEncodingsAndCodecs) throws ZarrException, IOException {
        // regression test: ensure that 'name' keyword of named configurations (e.g. codecs) are only written once.
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testZarrJsonFormatV3").resolve(String.valueOf(chunkKeyEncodingsAndCodecs.hashCode()));
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunkShape(6, 6);
        builder = chunkKeyEncodingsAndCodecs.apply(builder);

        Array.create(storeHandle, builder.build());

        try (BufferedReader reader = Files.newBufferedReader(storeHandle.resolve(ZARR_JSON).toPath())) {
            String jsonInString = reader.lines().collect(Collectors.joining(System.lineSeparator()));
            JsonNode JSON = new ObjectMapper().readTree(jsonInString);
            Assertions.assertEquals(JSON.toPrettyString(), jsonInString);
        }
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
    public void testUpdateAttributesBehavior() throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testUpdateAttributesBehaviorV3");
        ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT8)
                .withChunkShape(5, 5)
                .withAttributes(new Attributes(b -> b.set("key1", "val1")))
                .build();

        Array array1 = Array.create(storeHandle, arrayMetadata);
        Array array2 = array1.updateAttributes(attrs -> attrs.set("key2", "val2"));

        Assertions.assertNotSame(array1, array2);
        Assertions.assertEquals("val1", array1.metadata().attributes().get("key1"));
        Assertions.assertNull(array1.metadata().attributes().get("key2"));

        Assertions.assertEquals("val1", array2.metadata().attributes().get("key1"));
        Assertions.assertEquals("val2", array2.metadata().attributes().get("key2"));

        // Re-opening should show the updated attributes
        Array array3 = Array.open(storeHandle);
        Assertions.assertEquals("val2", array3.metadata().attributes().get("key2"));
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

        ucar.ma2.Array data = array.read(new long[]{0, 0}, new long[]{10, 10});
        Assertions.assertArrayEquals(testData, (int[]) data.get1DJavaArray(ma2DataType));

        data = array.read(new long[]{10, 10}, new long[]{5, 5});
        int[] expectedData = new int[5 * 5];
        Arrays.fill(expectedData, 1);
        Assertions.assertArrayEquals(expectedData, (int[]) data.get1DJavaArray(ma2DataType));

        Array reopenedArray = Array.open(storeHandle);
        Assertions.assertArrayEquals(new int[]{20, 15}, reopenedArray.read().getShape());
    }

    @Test
    public void testResizeArrayShrink() throws IOException, ZarrException {
        int[] testData = new int[10 * 10];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testResizeArrayShrinkV3");
        ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT32)
                .withChunkShape(5, 5)
                .build();
        ucar.ma2.DataType ma2DataType = arrayMetadata.dataType.getMA2DataType();
        Array array = Array.create(storeHandle, arrayMetadata);
        array.write(new long[]{0, 0}, ucar.ma2.Array.factory(ma2DataType, new int[]{10, 10}, testData));

        array = array.resize(new long[]{5, 5});
        Assertions.assertArrayEquals(new int[]{5, 5}, array.read().getShape());

        ucar.ma2.Array data = array.read();
        int[] expectedData = new int[5 * 5];
        for (int i = 0; i < 5; i++) {
            System.arraycopy(testData, i * 10 + 0, expectedData, i * 5 + 0, 5);
        }
        Assertions.assertArrayEquals(expectedData, (int[]) data.get1DJavaArray(ma2DataType));
    }

    @Test
    public void testResizeArrayShrinkWithChunkCleanup() throws IOException, ZarrException {
        int[] testData = new int[10 * 10];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testResizeArrayShrinkWithChunkCleanupV3");
        ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT32)
                .withChunkShape(5, 5)
                .withFillValue(99)
                .build();
        ucar.ma2.DataType ma2DataType = arrayMetadata.dataType.getMA2DataType();
        Array array = Array.create(storeHandle, arrayMetadata);
        array.write(new long[]{0, 0}, ucar.ma2.Array.factory(ma2DataType, new int[]{10, 10}, testData));

        // Verify all 4 chunks exist before resize (v3 default encoding has "c" prefix)
        Assertions.assertTrue(storeHandle.resolve("c", "0", "0").exists());
        Assertions.assertTrue(storeHandle.resolve("c", "0", "1").exists());
        Assertions.assertTrue(storeHandle.resolve("c", "1", "0").exists());
        Assertions.assertTrue(storeHandle.resolve("c", "1", "1").exists());

        // Resize with chunk cleanup (resizeMetadataOnly=false)
        array = array.resize(new long[]{5, 5}, false);
        Assertions.assertArrayEquals(new int[]{5, 5}, array.read().getShape());

        // Verify only chunk (0,0) still exists
        Assertions.assertTrue(storeHandle.resolve("c", "0", "0").exists());
        Assertions.assertFalse(storeHandle.resolve("c", "0", "1").exists());
        Assertions.assertFalse(storeHandle.resolve("c", "1", "0").exists());
        Assertions.assertFalse(storeHandle.resolve("c", "1", "1").exists());

        ucar.ma2.Array data = array.read();
        int[] expectedData = new int[5 * 5];
        for (int i = 0; i < 5; i++) {
            System.arraycopy(testData, i * 10 + 0, expectedData, i * 5 + 0, 5);
        }
        Assertions.assertArrayEquals(expectedData, (int[]) data.get1DJavaArray(ma2DataType));
    }

    @Test
    public void testResizeArrayShrinkWithBoundaryTrimming() throws IOException, ZarrException {
        int[] testData = new int[10 * 10];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testResizeArrayShrinkWithBoundaryTrimmingV3");
        ArrayMetadata arrayMetadata = Array.metadataBuilder()
                .withShape(10, 10)
                .withDataType(DataType.UINT32)
                .withChunkShape(5, 5)
                .withFillValue(99)
                .build();
        ucar.ma2.DataType ma2DataType = arrayMetadata.dataType.getMA2DataType();
        Array array = Array.create(storeHandle, arrayMetadata);
        array.write(new long[]{0, 0}, ucar.ma2.Array.factory(ma2DataType, new int[]{10, 10}, testData));

        // Resize to 7x7 (crosses chunk boundary, should trim boundary chunks)
        array = array.resize(new long[]{7, 7}, false);
        Assertions.assertArrayEquals(new int[]{7, 7}, array.read().getShape());

        // Verify chunks (0,0), (0,1), (1,0), (1,1) still exist (boundary trimmed, not deleted)
        Assertions.assertTrue(storeHandle.resolve("c", "0", "0").exists());
        Assertions.assertTrue(storeHandle.resolve("c", "0", "1").exists());
        Assertions.assertTrue(storeHandle.resolve("c", "1", "0").exists());
        Assertions.assertTrue(storeHandle.resolve("c", "1", "1").exists());

        // Now resize to expand again and check that trimmed area has fill value
        array = array.resize(new long[]{10, 10}, true);
        ucar.ma2.Array data = array.read(new long[]{7, 0}, new long[]{3, 10});
        // All values in rows 7-9 should be fill value (99)
        int[] expectedFillData = new int[3 * 10];
        Arrays.fill(expectedFillData, 99);
        Assertions.assertArrayEquals(expectedFillData, (int[]) data.get1DJavaArray(ma2DataType));
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

    @ParameterizedTest
    @MethodSource("unalignedArrayAccessProvider")
    public void testUnalignedArrayAccess(int arrayShape, int chunkShape, int accessShape) throws ZarrException, IOException {
        Array array = Array.create(
                new MemoryStore().resolve(),
                Array.metadataBuilder()
                        .withShape(arrayShape)
                        .withDataType(DataType.UINT32)
                        .withChunkShape(chunkShape)
                        .withFillValue(0)
                        .build()
        );

        int[] testData = new int[arrayShape];
        Arrays.setAll(testData, p -> (byte) p);
        ucar.ma2.Array data = ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{arrayShape}, testData);
        array.write(data);

        for (int i = 0; i < arrayShape; i += accessShape) {
            accessShape = Math.min(accessShape, arrayShape - i);
            ucar.ma2.Array result = array.read(new long[]{i}, new long[]{accessShape});
            int[] expectedData = Arrays.copyOfRange(testData, i, i + accessShape);
            Assertions.assertArrayEquals(expectedData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
        }
    }

    @Test
    public void testDefaultChunkShape() throws IOException, ZarrException {
        // Test with a small array (< 512 elements per dimension)
        Array smallArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("v3_default_chunks_small"),
                Array.metadataBuilder()
                        .withShape(100, 50)
                        .withDataType(DataType.UINT8)
                        .build()
        );
        Assertions.assertEquals(2, smallArray.metadata().chunkShape().length);
        // Both dimensions < 512, so chunks should equal shape
        Assertions.assertEquals(100, smallArray.metadata().chunkShape()[0]);
        Assertions.assertEquals(50, smallArray.metadata().chunkShape()[1]);

        // Test with a larger array (> 512 elements per dimension)
        Array largeArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("v3_default_chunks_large"),
                Array.metadataBuilder()
                        .withShape(2000, 1500)
                        .withDataType(DataType.UINT8)
                        .build()
        );
        Assertions.assertEquals(2, largeArray.metadata().chunkShape().length);
        // Chunks should be calculated based on division by 512
        Assertions.assertTrue(largeArray.metadata().chunkShape()[0] > 0);
        Assertions.assertTrue(largeArray.metadata().chunkShape()[0] < 2000);
        Assertions.assertTrue(largeArray.metadata().chunkShape()[1] > 0);
        Assertions.assertTrue(largeArray.metadata().chunkShape()[1] < 1500);

        // Test with mixed dimensions
        Array mixedArray = Array.create(
                new FilesystemStore(TESTOUTPUT).resolve("v3_default_chunks_mixed"),
                Array.metadataBuilder()
                        .withShape(1024, 100, 2048)
                        .withDataType(DataType.UINT8)
                        .build()
        );
        Assertions.assertEquals(3, mixedArray.metadata().chunkShape().length);
        // Verify chunks are reasonable
        Assertions.assertTrue(mixedArray.metadata().chunkShape()[0] > 0);
        Assertions.assertTrue(mixedArray.metadata().chunkShape()[0] <= 1024);
        Assertions.assertEquals(100, mixedArray.metadata().chunkShape()[1]);  // < 512, should equal shape
        Assertions.assertTrue(mixedArray.metadata().chunkShape()[2] > 0);
        Assertions.assertTrue(mixedArray.metadata().chunkShape()[2] <= 2048);
    }

    @Test
    public void testLargeArrayWithOffsetBeyondMaxInt() throws IOException, ZarrException {
        // Create an array with second dimension exceeding Integer.MAX_VALUE
        // Shape: [2, 3_000_000_000] - 3 billion elements in second dimension
        // This array is mostly fillvalue, only write one small chunk
        long largeSize = 3_000_000_000L; // 3 billion > Integer.MAX_VALUE (2.147B)

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("large_array_beyond_int");
        ArrayMetadata metadata = Array.metadataBuilder()
                .withShape(largeSize, largeSize)
                .withDataType(DataType.INT32)
                .withChunkShape(1000, 1000)
                .withFillValue(42)
                .build();

        Array array = Array.create(storeHandle, metadata);

        // Write a small chunk at position [0, 0]
        int[] testData = new int[1000];
        Arrays.fill(testData, 100);
        ucar.ma2.Array smallChunk = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, new int[]{1, 1000}, testData);
        array.write(new long[]{0, 0}, smallChunk);

        // Write a small chunk at position [1, Integer.MAX_VALUE + 1]
        long beyondIntMax = (long) (Integer.MAX_VALUE) + 1;
        long[] offset = new long[]{1, beyondIntMax};
        Arrays.fill(testData, 200);
        smallChunk = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, new int[]{1, 1000}, testData);
        array.write(offset, smallChunk);

        // Read from the written region - should get our data
        ucar.ma2.Array readStart = array.read(new long[]{0, 0}, new long[]{1, 100});
        Assertions.assertEquals(100, readStart.getInt(0), "Data at start should be written value");

        // Read from position beyond Integer.MAX_VALUE - should get fillvalue
        ucar.ma2.Array readFar = array.read(offset, new long[]{1, 100});
        Assertions.assertEquals(200, readFar.getInt(1), "Data beyond Integer.MAX_VALUE should be fillvalue");

        // Verify metadata
        Assertions.assertEquals(2, array.metadata().shape.length);
        Assertions.assertEquals(largeSize, array.metadata().shape[0]);
        Assertions.assertEquals(largeSize, array.metadata().shape[1]);
    }

    @ParameterizedTest
    @MethodSource("dataTypeAndEndianProvider")
    public void testEndianness(DataType dataType, BytesCodec.Endian endian) throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testEndiannessV3").resolve(dataType.name()).resolve(endian.name());
        ucar.ma2.Array testData = testdata(dataType);

        ArrayMetadata metadata = Array.metadataBuilder()
                .withShape(toLongArray(testData.getShape()))
                .withDataType(dataType)
                .withCodecs(c -> c.withBytes(endian))
                .build();
        Array array = Array.create(storeHandle, metadata);
        array.write(testData);
        Array reopenedArray = Array.open(storeHandle);
        ucar.ma2.Array readData = reopenedArray.read();
        assertIsTestdata(readData, dataType);
    }
}
