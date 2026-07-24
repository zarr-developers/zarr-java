package dev.zarr.zarrjava.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadataBuilder;
import dev.zarr.zarrjava.v3.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.Arrays;

public class CompressionCodecsTest extends ZarrTest {

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
}
