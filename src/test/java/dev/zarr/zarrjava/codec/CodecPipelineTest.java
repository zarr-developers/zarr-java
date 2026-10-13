package dev.zarr.zarrjava.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadataBuilder;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.Assert.assertThrows;

public class CodecPipelineTest extends ZarrTest {

    static Stream<Function<CodecBuilder, CodecBuilder>> invalidCodecBuilder() {
        return Stream.of(
                c -> c.withBytes(BytesCodec.Endian.LITTLE).withBytes(BytesCodec.Endian.LITTLE),
                c -> c.withBlosc().withBytes(BytesCodec.Endian.LITTLE),
                c -> c.withBytes(BytesCodec.Endian.LITTLE).withTranspose(new int[]{1, 0}),
                c -> c.withTranspose(new int[]{1, 0}).withBytes(BytesCodec.Endian.LITTLE).withTranspose(new int[]{1, 0})
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
}
