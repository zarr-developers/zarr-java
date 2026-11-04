package dev.zarr.zarrjava.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadataBuilder;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.core.JpegCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static dev.zarr.zarrjava.core.Node.ZARR_JSON;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JpegCodecTest extends ZarrTest {

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
                .withCodecs(c -> c.withJpeg(quality, "ycbcr", new int[][]{{2, 2}, {1, 1}, {1, 1}}));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, shape, testData));

        ucar.ma2.Array result = Array.open(storeHandle).read();
        byte[] roundTripped = (byte[]) result.copyTo1DJavaArray();
        // RGB goes through the YCbCr transform and 4:2:0 chroma subsampling, so a
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
    public void testJpegCodecRejectsColorChunkForGrayscaleConfig() throws ZarrException, IOException {
        // encoded_color_space "grayscale" fixes 1 component, so a 3-component chunk is rejected.
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
    public void testJpegCodecRejectsGrayscaleChunkForColorConfig() throws ZarrException, IOException {
        // encoded_color_space "ycbcr" fixes 3 components, so a 1-component chunk is rejected.
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
    public void testJpegCodecRejectsSubsamplingLengthMismatch() {
        // A subsampling array whose length does not match the component count is rejected.
        ZarrException ex = assertThrows(ZarrException.class,
                () -> new JpegCodec.Configuration(90, "ycbcr", new int[][]{{2, 2}, {1, 1}}));
        Assertions.assertTrue(ex.getMessage().contains("one entry per component"), ex.getMessage());
    }

    @ParameterizedTest
    @CsvSource({"rgb,grayscale", "grayscale,rgb", "rgb,ycbcr", "grayscale,ycbcr", "cmyk,rgb", "ycbcr,xyz"})
    public void testJpegCodecRejectsInvalidColorSpaceCombination(String encoded, String decoded) {
        int[][] subsampling = "grayscale".equals(encoded)
                ? new int[][]{{1, 1}} : new int[][]{{1, 1}, {1, 1}, {1, 1}};
        assertThrows(ZarrException.class,
                () -> new JpegCodec.Configuration(90, encoded, decoded, subsampling));
    }

    @Test
    public void testJpegCodecYCbCrPassthroughReadWrite() throws ZarrException, IOException {
        // decoded_color_space "ycbcr" stores YCbCr samples without conversion and returns them as-is.
        int[] shape = {16, 16, 3};
        byte[] testData = rgbRampData(shape[0] * shape[1]);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegYCbCrPassthrough");
        Array writeArray = Array.create(storeHandle, Array.metadataBuilder()
                .withShape(16, 16, 3)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16, 3)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(100, "ycbcr", "ycbcr", new int[][]{{1, 1}, {1, 1}, {1, 1}}))
                .build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UBYTE, shape, testData));

        String zarrJson = new String(Files.readAllBytes(
                Paths.get("testoutput", "testJpegYCbCrPassthrough", ZARR_JSON))).replaceAll("\\s+", "");
        Assertions.assertTrue(zarrJson.contains("\"decoded_color_space\":\"ycbcr\""), zarrJson);

        ucar.ma2.Array result = Array.open(storeHandle).read();
        byte[] roundTripped = (byte[]) result.copyTo1DJavaArray();
        // No color transform in either direction, so only DCT quantization loss remains.
        Assertions.assertTrue(maxAbsDiff(testData, roundTripped) <= 4,
                "ycbcr passthrough round-trip differs too much");
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
    public void testJpegCodecDefaultWritesFullConfiguration() throws ZarrException, IOException {
        // All required parameters are written explicitly, even for the default grayscale codec.
        // subsampling is only required for ycbcr and is omitted otherwise.
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegDefaultMetadata");
        Array.create(storeHandle, Array.metadataBuilder()
                .withShape(16, 16)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg())
                .build());

        String zarrJson = new String(Files.readAllBytes(
                Paths.get("testoutput", "testJpegDefaultMetadata", ZARR_JSON))).replaceAll("\\s+", "");
        Assertions.assertTrue(zarrJson.contains("\"quality\":95"), zarrJson);
        Assertions.assertTrue(zarrJson.contains("\"encoded_color_space\":\"grayscale\""), zarrJson);
        Assertions.assertFalse(zarrJson.contains("subsampling"), zarrJson);
        // decoded_color_space is omitted when it is the default for the encoded color space.
        Assertions.assertFalse(zarrJson.contains("decoded_color_space"), zarrJson);
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "\"quality\":90,|''",
            ",\"encoded_color_space\":\"grayscale\"|''",
            "\"configuration\":{\"quality\":90,\"encoded_color_space\":\"grayscale\"}|\"configuration\":null"})
    public void testJpegCodecRejectsMissingRequiredParameter(String present, String replacement)
            throws ZarrException, IOException {
        // quality, encoded_color_space and the configuration itself are required.
        String name = "testJpegRejectsMissing" + Math.abs(present.hashCode());
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve(name);
        Array.create(storeHandle, Array.metadataBuilder()
                .withShape(16, 16)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(90))
                .build());

        Path zarrJsonPath = Paths.get("testoutput", name, ZARR_JSON);
        String zarrJson = new String(Files.readAllBytes(zarrJsonPath)).replaceAll("\\s+", "");
        Assertions.assertTrue(zarrJson.contains(present), zarrJson);
        Files.write(zarrJsonPath, zarrJson.replace(present, replacement).getBytes());

        assertThrows(Exception.class, () -> Array.open(storeHandle));
    }

    @Test
    public void testJpegCodecRgbOmitsSubsampling() throws ZarrException, IOException {
        // With rgb (and grayscale), subsampling defaults to no subsampling and is not written.
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testJpegRgbMetadata");
        Array.create(storeHandle, Array.metadataBuilder()
                .withShape(16, 16, 3)
                .withDataType(DataType.UINT8)
                .withChunkShape(16, 16, 3)
                .withFillValue(0)
                .withCodecs(c -> c.withJpeg(90, "rgb"))
                .build());

        String zarrJson = new String(Files.readAllBytes(
                Paths.get("testoutput", "testJpegRgbMetadata", ZARR_JSON))).replaceAll("\\s+", "");
        Assertions.assertTrue(zarrJson.contains("\"encoded_color_space\":\"rgb\""), zarrJson);
        Assertions.assertFalse(zarrJson.contains("subsampling"), zarrJson);
        Assertions.assertNotNull(Array.open(storeHandle));
    }

    @ParameterizedTest
    @CsvSource({"grayscale", "rgb"})
    public void testJpegCodecDefaultsSubsamplingWhenOmitted(String encoded) throws ZarrException {
        int[][] expected = "grayscale".equals(encoded)
                ? new int[][]{{1, 1}} : new int[][]{{1, 1}, {1, 1}, {1, 1}};
        JpegCodec.Configuration omitted = new JpegCodec.Configuration(90, encoded, null);
        Assertions.assertArrayEquals(expected, omitted.subsampling);

        // An explicit subsampling equal to the default is still accepted.
        int[][] explicit = expected;
        Assertions.assertDoesNotThrow(() -> new JpegCodec.Configuration(90, encoded, explicit));
    }

    @Test
    public void testJpegCodecRequiresSubsamplingForYCbCr() {
        ZarrException ex = assertThrows(ZarrException.class,
                () -> new JpegCodec.Configuration(90, "ycbcr", null));
        Assertions.assertTrue(ex.getMessage().contains("subsampling"), ex.getMessage());
    }

    @ParameterizedTest
    @CsvSource({"0", "101", "-1"})
    public void testJpegCodecRejectsQualityOutOfRange(int quality) {
        assertThrows(ZarrException.class, () -> new JpegCodec.Configuration(quality));
    }

    @Test
    public void testJpegCodecRejectsSubsampledGrayscale() {
        // Grayscale data must use subsampling [[1, 1]].
        ZarrException ex = assertThrows(ZarrException.class,
                () -> new JpegCodec.Configuration(90, "grayscale", new int[][]{{2, 2}}));
        Assertions.assertTrue(ex.getMessage().contains("grayscale"), ex.getMessage());
    }
}
