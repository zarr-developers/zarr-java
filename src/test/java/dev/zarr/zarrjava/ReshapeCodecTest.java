package dev.zarr.zarrjava;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.v3.ArrayMetadataBuilder;
import dev.zarr.zarrjava.v3.DataType;
import dev.zarr.zarrjava.v3.codec.core.ReshapeCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.MAMath;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.assertThrows;

public class ReshapeCodecTest extends ZarrTest {

    /**
     * Builds a {@link ReshapeCodec} whose input array is a single chunk of shape {@code inputShape}.
     */
    private static ReshapeCodec reshapeCodec(Object[] shape, int[] inputShape) throws ZarrException {
        ReshapeCodec codec = new ReshapeCodec(new ReshapeCodec.Configuration(shape));
        codec.setCoreArrayMetadata(new ArrayMetadata.CoreArrayMetadata(
                Arrays.stream(inputShape).asLongStream().toArray(),
                inputShape,
                DataType.UINT32,
                null));
        return codec;
    }

    /**
     * The elements of {@code array} in lexicographical (C) order. Walks the array with an
     * {@link ucar.ma2.IndexIterator} rather than {@code get1DJavaArray}, so that it stays an
     * independent oracle for the codec, which uses the latter itself.
     */
    private static int[] ravel(ucar.ma2.Array array) {
        int[] elements = new int[(int) array.getSize()];
        ucar.ma2.IndexIterator iter = array.getIndexIterator();
        for (int i = 0; i < elements.length; i++) {
            elements[i] = iter.getIntNext();
        }
        return elements;
    }

    private static ucar.ma2.Array sequential(int[] shape) {
        int size = Arrays.stream(shape).reduce(1, (a, b) -> a * b);
        int[] data = new int[size];
        Arrays.setAll(data, p -> p);
        return ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, shape, data);
    }

    static Stream<Arguments> validReshapes() {
        return Stream.of(
                // merge two leading dims
                Arguments.of(new int[]{2, 3, 4}, new Object[]{new int[]{0, 1}, new int[]{2}}, new int[]{6, 4}),
                // flatten everything with an explicit product-of-all
                Arguments.of(new int[]{2, 3, 4}, new Object[]{new int[]{0, 1, 2}}, new int[]{24}),
                // flatten everything with -1
                Arguments.of(new int[]{2, 3, 4}, new Object[]{-1}, new int[]{24}),
                // spec example (scaled down): [[0,1],[2],3]
                Arguments.of(new int[]{4, 5, 6, 3}, new Object[]{new int[]{0, 1}, new int[]{2}, 3}, new int[]{20, 6, 3}),
                // split a dimension with explicit sizes
                Arguments.of(new int[]{6, 4}, new Object[]{2, 3, 4}, new int[]{2, 3, 4}),
                // introduce a leading singleton dimension (squeeze/unsqueeze use case)
                Arguments.of(new int[]{4, 4}, new Object[]{1, new int[]{0}, new int[]{1}}, new int[]{1, 4, 4}),
                // -1 combined with an explicit size
                Arguments.of(new int[]{2, 3, 4}, new Object[]{6, -1}, new int[]{6, 4}),
                // -1 combined with input_dims
                Arguments.of(new int[]{2, 3, 4}, new Object[]{new int[]{0}, -1}, new int[]{2, 12}),
                // identity reshape
                Arguments.of(new int[]{2, 3, 4}, new Object[]{new int[]{0}, new int[]{1}, new int[]{2}}, new int[]{2, 3, 4}),
                // trailing singleton dimension
                Arguments.of(new int[]{2, 3}, new Object[]{new int[]{0}, new int[]{1}, 1}, new int[]{2, 3, 1}),
                // high-dimensional -> 1D (motivating zfp/image use case)
                Arguments.of(new int[]{2, 2, 2, 2, 2}, new Object[]{-1}, new int[]{32})
        );
    }

    @ParameterizedTest
    @MethodSource("validReshapes")
    public void testReshapeRoundTrip(int[] inputShape, Object[] shape, int[] expectedOutputShape)
            throws ZarrException {
        ReshapeCodec codec = reshapeCodec(shape, inputShape);

        ucar.ma2.Array input = sequential(inputShape);
        ucar.ma2.Array encoded = codec.encode(input);

        // Output shape matches the specification.
        Assertions.assertArrayEquals(expectedOutputShape, encoded.getShape());
        // The lexicographical (C-order) ravel is preserved: ravel(B) == ravel(A).
        Assertions.assertArrayEquals(ravel(input), ravel(encoded));

        // decode is the inverse of encode.
        ucar.ma2.Array decoded = codec.decode(encoded);
        Assertions.assertArrayEquals(inputShape, decoded.getShape());
        Assertions.assertTrue(MAMath.equals(input, decoded));
    }

    @Test
    public void testResolveArrayMetadataMergesChunkGrid() throws ZarrException {
        // 8x6 array, 4x3 chunks -> 2x2 chunk grid. Merging both dims yields a 1D chunk of 12 elements
        // and the chunk counts multiply into the single output dimension: 4 chunks * 12 = 48.
        ReshapeCodec codec = new ReshapeCodec(new ReshapeCodec.Configuration(new Object[]{new int[]{0, 1}}));
        codec.setCoreArrayMetadata(new ArrayMetadata.CoreArrayMetadata(
                new long[]{8, 6},
                new int[]{4, 3},
                DataType.UINT32,
                null));

        ArrayMetadata.CoreArrayMetadata resolved = codec.resolveArrayMetadata();
        Assertions.assertArrayEquals(new int[]{12}, resolved.chunkShape);
        Assertions.assertArrayEquals(new long[]{48}, resolved.shape);
    }

    @Test
    public void testResolveArrayMetadataKeepsGridPerOutputDim() throws ZarrException {
        // 8x6x4 array, 4x3x4 chunks -> 2x2x1 chunk grid. shape=[[0,1],[2]] merges the first two dims;
        // their chunk counts (2*2=4) attach to output dim 0, dim 1 keeps its single chunk.
        ReshapeCodec codec = new ReshapeCodec(
                new ReshapeCodec.Configuration(new Object[]{new int[]{0, 1}, new int[]{2}}));
        codec.setCoreArrayMetadata(new ArrayMetadata.CoreArrayMetadata(
                new long[]{8, 6, 4},
                new int[]{4, 3, 4},
                DataType.UINT32,
                null));

        ArrayMetadata.CoreArrayMetadata resolved = codec.resolveArrayMetadata();
        Assertions.assertArrayEquals(new int[]{12, 4}, resolved.chunkShape);
        Assertions.assertArrayEquals(new long[]{48, 4}, resolved.shape);
    }

    static Stream<Arguments> invalidReshapes() {
        return Stream.of(
                // product mismatch
                Arguments.of(new int[]{2, 3}, new Object[]{5}),
                Arguments.of(new int[]{2, 3, 4}, new Object[]{7, -1}),
                // more than one -1
                Arguments.of(new int[]{2, 3, 4}, new Object[]{-1, -1}),
                // zero / negative explicit sizes
                Arguments.of(new int[]{2, 3}, new Object[]{0, 6}),
                Arguments.of(new int[]{2, 3}, new Object[]{-2, 3}),
                // input dims not strictly increasing (would suggest a transpose)
                Arguments.of(new int[]{2, 3}, new Object[]{new int[]{1}, new int[]{0}}),
                Arguments.of(new int[]{2, 3, 4}, new Object[]{new int[]{1, 0}, new int[]{2}}),
                // duplicate input dim
                Arguments.of(new int[]{2, 3}, new Object[]{new int[]{0, 0}}),
                // input dim out of range
                Arguments.of(new int[]{2, 3}, new Object[]{new int[]{5}}),
                // input_dims placed such that the ravel would not align (implicit transpose)
                Arguments.of(new int[]{2, 2, 2}, new Object[]{new int[]{2}, 4}),
                // empty shape
                Arguments.of(new int[]{2, 3}, new Object[]{})
        );
    }

    @ParameterizedTest
    @MethodSource("invalidReshapes")
    public void testReshapeInvalidConfig(int[] inputShape, Object[] shape) {
        // Invalid configurations are rejected eagerly when the array metadata is set (i.e. at pipeline
        // construction time), not lazily on the first encode/decode.
        assertThrows(ZarrException.class, () -> reshapeCodec(shape, inputShape));
    }

    @Test
    public void testReshapeConstructsViewForLexicographicalInput() throws ZarrException {
        // A freshly allocated array already walks its backing store in lexicographical order, so the
        // reshape must be a virtual view: the result has to share storage with the input rather than
        // copy it. This is the requirement the specification states as "implementations should, when
        // possible, construct a virtual view rather than copy the array".
        ReshapeCodec codec = reshapeCodec(new Object[]{new int[]{0, 1}, new int[]{2}}, new int[]{2, 3, 4});

        ucar.ma2.Array input = sequential(new int[]{2, 3, 4});
        ucar.ma2.Array encoded = codec.encode(input);

        Assertions.assertArrayEquals(new int[]{6, 4}, encoded.getShape());
        Assertions.assertSame(input.getStorage(), encoded.getStorage());
        // The view is itself in lexicographical order, so decoding it stays a view as well.
        Assertions.assertSame(input.getStorage(), codec.decode(encoded).getStorage());
    }

    @Test
    public void testReshapeCopiesPermutedInputInRavelOrder() throws ZarrException {
        // The transpose codec hands on a permuted view, whose elements are not laid out in ravel
        // order. No shape/stride/offset descriptor over the original store can express that ravel, so
        // a copy is mandatory here -- reshapeNoCopy would silently return the elements in store order.
        ReshapeCodec codec = reshapeCodec(new Object[]{-1}, new int[]{4, 3, 2});

        ucar.ma2.Array permuted = sequential(new int[]{2, 3, 4}).permute(new int[]{2, 1, 0});
        ucar.ma2.Array encoded = codec.encode(permuted);

        Assertions.assertArrayEquals(new int[]{24}, encoded.getShape());
        Assertions.assertNotSame(permuted.getStorage(), encoded.getStorage());
        Assertions.assertArrayEquals(ravel(permuted), ravel(encoded));
    }

    @Test
    public void testReshapePreservesRavelOfStridedSection() throws ZarrException, InvalidRangeException {
        // Array.write passes a section of the caller's array for every chunk of a multi-chunk write.
        // Whether that can stay a view depends on the strides, so only the ravel is asserted here.
        ReshapeCodec codec = reshapeCodec(new Object[]{new int[]{0, 1}, new int[]{2}}, new int[]{2, 3, 4});

        ucar.ma2.Array section = sequential(new int[]{4, 3, 4})
                .sectionNoReduce(new int[]{1, 0, 0}, new int[]{2, 3, 4}, null);
        ucar.ma2.Array encoded = codec.encode(section);

        Assertions.assertArrayEquals(new int[]{6, 4}, encoded.getShape());
        Assertions.assertArrayEquals(ravel(section), ravel(encoded));
    }

    @Test
    public void testEncodeRejectsWrongInputShape() throws ZarrException {
        ReshapeCodec codec = reshapeCodec(new Object[]{new int[]{0, 1}}, new int[]{2, 3});
        ucar.ma2.Array wrong = sequential(new int[]{3, 3});
        assertThrows(ZarrException.class, () -> codec.encode(wrong));
    }

    @Test
    public void testReshapeCodecReadWriteSingleChunk() throws ZarrException, IOException {
        int[] shape = new int[]{4, 5, 6, 3};
        int[] testData = new int[4 * 5 * 6 * 3];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("reshape_single_chunk");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(4, 5, 6, 3)
                .withDataType(DataType.UINT32)
                .withChunkShape(4, 5, 6, 3)
                .withFillValue(0)
                .withCodecs(c -> c.withReshape(new Object[]{new int[]{0, 1}, new int[]{2}, 3}).withZstd());
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, shape, testData));

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
    }

    @Test
    public void testReshapeCodecReadWriteMultipleChunks() throws ZarrException, IOException {
        int[] shape = new int[]{8, 6, 4};
        int[] testData = new int[8 * 6 * 4];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("reshape_multi_chunk");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(8, 6, 4)
                .withDataType(DataType.UINT32)
                .withChunkShape(4, 3, 4)
                .withFillValue(0)
                // Fold the 3D chunk into a 2D chunk, a typical image-codec preparation step.
                .withCodecs(c -> c.withReshape(new Object[]{new int[]{0, 1}, new int[]{2}}).withBytes("LITTLE"));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, shape, testData));

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
    }

    @Test
    public void testReshapeCombinedWithTranspose() throws ZarrException, IOException {
        int[] shape = new int[]{4, 4, 4};
        int[] testData = new int[4 * 4 * 4];
        Arrays.setAll(testData, p -> p);

        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("reshape_with_transpose");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(4, 4, 4)
                .withDataType(DataType.UINT32)
                .withChunkShape(4, 4, 4)
                .withFillValue(0)
                // transpose reorders, reshape then folds — the spec's intended combination.
                .withCodecs(c -> c.withTranspose(new int[]{2, 1, 0})
                        .withReshape(new Object[]{new int[]{0, 1}, new int[]{2}})
                        .withBytes("LITTLE"));
        Array writeArray = Array.create(storeHandle, builder.build());
        writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, shape, testData));

        Array readArray = Array.open(storeHandle);
        ucar.ma2.Array result = readArray.read();

        Assertions.assertArrayEquals(testData, (int[]) result.get1DJavaArray(ucar.ma2.DataType.UINT));
    }

    @Test
    public void testInvalidReshapeFailsOnWrite() {
        StoreHandle storeHandle = new FilesystemStore(TESTOUTPUT).resolve("reshape_invalid_write");
        ArrayMetadataBuilder builder = Array.metadataBuilder()
                .withShape(4, 4)
                .withDataType(DataType.UINT32)
                .withChunkShape(4, 4)
                .withFillValue(0)
                // product 5 != 16
                .withCodecs(c -> c.withReshape(new Object[]{5}).withBytes("LITTLE"));

        assertThrows(ZarrException.class, () -> {
            Array writeArray = Array.create(storeHandle, builder.build());
            int[] testData = new int[16];
            writeArray.write(ucar.ma2.Array.factory(ucar.ma2.DataType.UINT, new int[]{4, 4}, testData));
        });
    }
}
