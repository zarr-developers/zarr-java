package dev.zarr.zarrjava.codec;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.ZarrTest;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static dev.zarr.zarrjava.core.Node.ZARR_JSON;

public class BytesCodecTest extends ZarrTest {

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
    @MethodSource("dataTypeAndEndianProvider")
    public void testEndianness(DataType dataType, BytesCodec.Endian endian) throws IOException, ZarrException {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("testEndiannessV3").resolve(dataType.name()).resolve(endian.name());
        assertCodecRoundtrip(storeHandle, dataType, c -> c.withBytes(endian));
    }
}
