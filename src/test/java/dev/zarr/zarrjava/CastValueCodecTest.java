package dev.zarr.zarrjava;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.core.CastValueCodec;
import dev.zarr.zarrjava.v3.codec.core.CastValueCodec.OutOfRange;
import dev.zarr.zarrjava.v3.codec.core.CastValueCodec.Rounding;
import dev.zarr.zarrjava.v3.codec.core.CastValueCodec.ScalarMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.Assert.assertThrows;

public class CastValueCodecTest extends ZarrTest {

    private StoreHandle handle(String name) {
        return new FilesystemStore(TESTOUTPUT).resolve("cast_value", name);
    }

    private double[] roundTrip(String name, DataType target, CastValueCodec.Configuration config,
                               double[] data) throws ZarrException, IOException {
        return roundTrip(name, target, config, data, 0);
    }

    private double[] roundTrip(String name, DataType target, CastValueCodec.Configuration config,
                               double[] data, Object fillValue) throws ZarrException, IOException {
        StoreHandle storeHandle = handle(name);
        ArrayMetadata metadata = Array.metadataBuilder()
                .withShape(data.length)
                .withDataType(DataType.FLOAT64)
                .withChunkShape(data.length)
                .withFillValue(fillValue)
                .withCodecs(c -> c.withCastValue(config).withBytes())
                .build();
        Array writeArray = Array.create(storeHandle, metadata);
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.DOUBLE, new int[]{data.length}, data));

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();
        return (double[]) result.get1DJavaArray(ucar.ma2.DataType.DOUBLE);
    }

    private static CastValueCodec.Configuration config(DataType target, Rounding rounding,
                                                       OutOfRange outOfRange, ScalarMap scalarMap) {
        return new CastValueCodec.Configuration(target, rounding, outOfRange, scalarMap);
    }

    @Test
    public void testExactRoundTrip() throws Exception {
        double[] data = {0, 1, 2, -3, 127, -128};
        double[] result = roundTrip("exact_int8", DataType.INT8,
                config(DataType.INT8, null, null, null), data);
        Assertions.assertArrayEquals(data, result);
    }

    @Test
    public void testClamp() throws Exception {
        double[] data = {200, -200, 50};
        double[] result = roundTrip("clamp_int8", DataType.INT8,
                config(DataType.INT8, null, OutOfRange.CLAMP, null), data);
        Assertions.assertArrayEquals(new double[]{127, -128, 50}, result);
    }

    @Test
    public void testWrap() throws Exception {
        double[] data = {130, -129, 261};
        double[] result = roundTrip("wrap_int8", DataType.INT8,
                config(DataType.INT8, null, OutOfRange.WRAP, null), data);
        Assertions.assertArrayEquals(new double[]{-126, 127, 5}, result);
    }

    @Test
    public void testRoundingNearestEven() throws Exception {
        double[] data = {2.5, 3.5, -2.5, 0.5};
        double[] result = roundTrip("round_even", DataType.INT32,
                config(DataType.INT32, Rounding.NEAREST_EVEN, null, null), data);
        Assertions.assertArrayEquals(new double[]{2, 4, -2, 0}, result);
    }

    @Test
    public void testRoundingTowardsZero() throws Exception {
        double[] data = {2.5, 3.5, -2.5, -3.9};
        double[] result = roundTrip("round_zero", DataType.INT32,
                config(DataType.INT32, Rounding.TOWARDS_ZERO, null, null), data);
        Assertions.assertArrayEquals(new double[]{2, 3, -2, -3}, result);
    }

    @Test
    public void testRoundingFloorAndCeil() throws Exception {
        double[] data = {2.1, -2.1};
        double[] floor = roundTrip("round_floor", DataType.INT32,
                config(DataType.INT32, Rounding.TOWARDS_NEGATIVE, null, null), data);
        Assertions.assertArrayEquals(new double[]{2, -3}, floor);
        double[] ceil = roundTrip("round_ceil", DataType.INT32,
                config(DataType.INT32, Rounding.TOWARDS_POSITIVE, null, null), data);
        Assertions.assertArrayEquals(new double[]{3, -2}, ceil);
    }

    @Test
    public void testNumpyCompatibilityExample() throws Exception {
        // Mirrors the specification's NumPy-compatibility example: float64 -> uint8, round towards
        // zero, wrap out-of-range values, and map NaN / +-Infinity to 0.
        ScalarMap scalarMap = new ScalarMap(
                new Object[][]{{"NaN", 0}, {"+Infinity", 0}, {"-Infinity", 0}}, null);
        double[] data = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 3.9, 300, -1};
        double[] result = roundTrip("numpy", DataType.UINT8,
                config(DataType.UINT8, Rounding.TOWARDS_ZERO, OutOfRange.WRAP, scalarMap), data);
        Assertions.assertArrayEquals(new double[]{0, 0, 0, 3, 44, 255}, result);
    }

    @Test
    public void testScalarMapDecode() throws Exception {
        // Round-trip a NaN fill/value: encode NaN -> 0 and decode 0 -> NaN.
        ScalarMap scalarMap = new ScalarMap(
                new Object[][]{{"NaN", 0}},
                new Object[][]{{0, "NaN"}});
        double[] data = {Double.NaN, 5};
        double[] result = roundTrip("scalarmap_decode", DataType.UINT8,
                config(DataType.UINT8, Rounding.TOWARDS_ZERO, OutOfRange.WRAP, scalarMap), data, 7);
        Assertions.assertTrue(Double.isNaN(result[0]));
        Assertions.assertEquals(5.0, result[1]);
    }

    @Test
    public void testOutOfRangeWithoutRuleFailsOnWrite() {
        double[] data = {200};
        // The write path runs on a parallel stream, so the codec's ZarrException surfaces wrapped.
        assertCastError(() -> roundTrip("oor_fail", DataType.INT8,
                config(DataType.INT8, null, null, null), data));
    }

    @Test
    public void testNaNToIntegerWithoutMappingFailsOnWrite() {
        double[] data = {Double.NaN};
        assertCastError(() -> roundTrip("nan_fail", DataType.INT8,
                config(DataType.INT8, null, OutOfRange.CLAMP, null), data));
    }

    private static void assertCastError(org.junit.function.ThrowingRunnable runnable) {
        Throwable thrown = assertThrows(Throwable.class, runnable);
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            if (t instanceof ZarrException) {
                return;
            }
        }
        Assertions.fail("Expected a ZarrException in the cause chain, got: " + thrown);
    }

    @Test
    public void testFillValueRoundTripValidation() {
        // Fill value NaN encodes to 0 but decodes back to 0.0 (no decode mapping), so it cannot
        // survive a round trip -> array creation must fail.
        StoreHandle storeHandle = handle("fill_roundtrip_fail");
        ScalarMap scalarMap = new ScalarMap(new Object[][]{{"NaN", 0}}, null);
        assertThrows(ZarrException.class, () -> {
            ArrayMetadata metadata = Array.metadataBuilder()
                    .withShape(4)
                    .withDataType(DataType.FLOAT64)
                    .withChunkShape(4)
                    .withFillValue("NaN")
                    .withCodecs(c -> c.withCastValue(
                            config(DataType.UINT8, Rounding.TOWARDS_ZERO, OutOfRange.WRAP, scalarMap)).withBytes())
                    .build();
            Array.create(storeHandle, metadata);
        });
    }

    @Test
    public void testFloat64ToFloat32RoundTrip() throws Exception {
        // Values that are exactly representable in float32 survive the round trip unchanged.
        double[] data = {0, 0.5, -0.25, 1024, -2048};
        double[] result = roundTrip("float32", DataType.FLOAT32,
                config(DataType.FLOAT32, null, null, null), data);
        Assertions.assertArrayEquals(data, result);
    }

    @Test
    public void testUnsupportedBoolTargetFails() {
        double[] data = {0, 1};
        assertThrows(ZarrException.class, () -> roundTrip("bool_fail", DataType.BOOL,
                config(DataType.BOOL, null, null, null), data));
    }
}
