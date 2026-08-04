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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.Assert.assertThrows;

public class ShardingCodecTest extends ZarrTest {

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

    @Test
    public void testShardingWithZstdCodecReadWrite() throws ZarrException, IOException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testShardingWithZstdCodecReadWrite");
        assertCodecRoundtrip(storeHandle, DataType.UINT32, new int[]{8, 8, 8},
                c -> c.withSharding(new int[]{2, 4, 8}, c1 -> c1.withZstd()));
    }

    @Test
    public void testShardingReadCutout() throws IOException, ZarrException {
        Array array = Array.open(new FilesystemStore(TESTDATA).resolve("l4_sample", "color", "1"));

        ucar.ma2.Array outArray = array.read(new long[]{0, 3073, 3073, 513}, new long[]{1, 64, 64, 64});
        Assertions.assertEquals(64 * 64 * 64, outArray.getSize());
        Assertions.assertEquals(-98, outArray.getByte(0));
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
}
