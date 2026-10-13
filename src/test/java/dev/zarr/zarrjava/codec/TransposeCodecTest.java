package dev.zarr.zarrjava.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.core.TransposeCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ucar.ma2.MAMath;

import java.util.stream.Stream;

import static org.junit.Assert.assertThrows;

public class TransposeCodecTest extends ZarrTest {

    static Stream<int[]> invalidTransposeOrder() {
        return Stream.of(
                new int[]{1, 0, 0},
                new int[]{1, 2, 3},
                new int[]{1, 2, 3, 0},
                new int[]{1, 2}
        );
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
}
