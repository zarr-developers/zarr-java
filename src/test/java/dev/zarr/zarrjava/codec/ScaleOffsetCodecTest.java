package dev.zarr.zarrjava.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.ArrayMetadataBuilder;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.core.ScaleOffsetCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ucar.ma2.MAMath;

import java.io.IOException;

import static dev.zarr.zarrjava.core.ArrayMetadata.parseFillValue;
import static dev.zarr.zarrjava.utils.Utils.toLongArray;
import static org.junit.Assert.assertThrows;

public class ScaleOffsetCodecTest extends ZarrTest {

    private static ScaleOffsetCodec scaleOffsetCodec(Object offset, Object scale, DataType dataType,
                                                     Object fillValue, int[] shape) throws ZarrException {
        ScaleOffsetCodec codec = new ScaleOffsetCodec(new ScaleOffsetCodec.Configuration(offset, scale));
        codec.setCoreArrayMetadata(new ArrayMetadata.CoreArrayMetadata(
                toLongArray(shape), shape, dataType, parseFillValue(fillValue, dataType)));
        return codec;
    }

    @Test
    public void testScaleOffsetCodecFloat() throws ZarrException {
        // scale 0.5 is exactly representable in float32, so the round-trip is lossless here.
        ucar.ma2.Array in = ucar.ma2.Array.factory(ucar.ma2.DataType.FLOAT, new int[]{4},
                new float[]{4.0f, 5.0f, 6.0f, 8.0f});
        // (x - 5) * 0.5
        ucar.ma2.Array encoded = ucar.ma2.Array.factory(ucar.ma2.DataType.FLOAT, new int[]{4},
                new float[]{-0.5f, 0.0f, 0.5f, 1.5f});

        ScaleOffsetCodec codec = scaleOffsetCodec(5, 0.5, DataType.FLOAT32, null, new int[]{4});
        assert MAMath.equals(encoded, codec.encode(in.copy()));
        assert MAMath.equals(in, codec.decode(encoded.copy()));
    }

    @Test
    public void testScaleOffsetCodecUintOffsetOnly() throws ZarrException {
        // Range reduction: subtract 1000, leaving values that fit in a byte. scale defaults to 1.
        ucar.ma2.Array in = ucar.ma2.Array.factory(ucar.ma2.DataType.USHORT, new int[]{4},
                new short[]{1000, 1001, 1050, 1255});
        ucar.ma2.Array encoded = ucar.ma2.Array.factory(ucar.ma2.DataType.USHORT, new int[]{4},
                new short[]{0, 1, 50, 255});

        ScaleOffsetCodec codec = scaleOffsetCodec(1000, null, DataType.UINT16, null, new int[]{4});
        assert MAMath.equals(encoded, codec.encode(in.copy()));
        assert MAMath.equals(in, codec.decode(encoded.copy()));
    }

    @Test
    public void testScaleOffsetCodecNoOp() throws ZarrException {
        ucar.ma2.Array in = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, new int[]{3},
                new int[]{-7, 0, 42});
        ScaleOffsetCodec codec = new ScaleOffsetCodec(null);
        codec.setCoreArrayMetadata(new ArrayMetadata.CoreArrayMetadata(
                new long[]{3}, new int[]{3}, DataType.INT32, null));
        assert MAMath.equals(in, codec.encode(in.copy()));
        assert MAMath.equals(in, codec.decode(in.copy()));
    }

    @Test
    public void testScaleOffsetCodecFillValueTransform() throws ZarrException {
        // The fill value is transformed with the encode formula and reported downstream.
        ScaleOffsetCodec codec = scaleOffsetCodec(5, 0.5, DataType.FLOAT32, 5.0f, new int[]{4});
        Object resolvedFill = codec.resolveArrayMetadata().parsedFillValue;
        Assertions.assertEquals(0.0f, resolvedFill);

        ScaleOffsetCodec uintCodec = scaleOffsetCodec(1000, null, DataType.UINT16, 1000, new int[]{4});
        Assertions.assertEquals((short) 0, uintCodec.resolveArrayMetadata().parsedFillValue);
    }

    @Test
    public void testScaleOffsetCodecIntegerOutOfRangeIsError() throws ZarrException {
        // 500 - 1000 = -500 is not representable in uint16 -> hard error (no numpy-style wraparound).
        ucar.ma2.Array in = ucar.ma2.Array.factory(ucar.ma2.DataType.USHORT, new int[]{1},
                new short[]{500});
        ScaleOffsetCodec codec = scaleOffsetCodec(1000, null, DataType.UINT16, null, new int[]{1});
        assertThrows(ZarrException.class, () -> codec.encode(in));
    }

    @Test
    public void testScaleOffsetCodecNonExactDivisionIsError() throws ZarrException {
        // Decoding requires in / scale to be an exact integer for integral data types.
        ucar.ma2.Array stored = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, new int[]{1},
                new int[]{5});
        ScaleOffsetCodec codec = scaleOffsetCodec(0, 10, DataType.INT32, null, new int[]{1});
        assertThrows(ZarrException.class, () -> codec.decode(stored));
    }

    @Test
    public void testScaleOffsetCodecReadWrite() throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testScaleOffsetCodecReadWrite");
        float[] values = new float[16 * 16 * 16];
        for (int i = 0; i < values.length; i++) {
            // multiples of 0.5, all exactly representable and exactly recoverable with scale 0.5
            values[i] = (i % 32) * 0.5f + 3.0f;
        }
        ucar.ma2.Array testData = ucar.ma2.Array.factory(ucar.ma2.DataType.FLOAT,
                new int[]{16, 16, 16}, values);

        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(16, 16, 16)
                .withDataType(DataType.FLOAT32)
                .withChunkShape(4, 8, 16)
                .withFillValue(3.0f)
                .withCodecs(c -> c.withScaleOffset(3.0, 0.5));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(testData);

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();
        assert MAMath.equals(testData, result);
    }
}
