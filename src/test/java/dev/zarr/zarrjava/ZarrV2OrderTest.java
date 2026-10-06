package dev.zarr.zarrjava;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.v2.Array;
import dev.zarr.zarrjava.v2.ArrayMetadata;
import dev.zarr.zarrjava.v2.DataType;
import dev.zarr.zarrjava.v2.Order;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests for the Zarr v2 {@code order} key, which selects whether a chunk is serialized row-major
 * ({@code "C"}) or column-major ({@code "F"}).
 * <p>
 * These tests are deliberately built around the committed golden fixtures in
 * {@code testdata/golden/}, which were produced by zarr-python. A Java-only round-trip cannot detect
 * a wrong {@code order} implementation: the writer and the reader shared the same bug, so they
 * agreed with each other while disagreeing with every other Zarr implementation. Comparing against
 * bytes written by the reference implementation is what makes these tests meaningful, and doing it
 * against committed fixtures keeps them fast and Python-free.
 *
 * @see dev.zarr.zarrjava.v2.codec.core.FortranOrderCodec
 */
public class ZarrV2OrderTest extends ZarrTest {

    static final Path GOLDEN = TESTDATA.resolve("golden");

    /**
     * The golden fixtures, as (fixture name, shape, chunks, order). Every fixture holds
     * {@code arange(prod(shape))} as {@code int32} with no compressor, so its chunk files are the raw
     * element bytes.
     */
    static Stream<Arguments> goldenFixtures() {
        return Stream.of(
                Arguments.of("v2_order_c", new long[]{3, 4}, new int[]{3, 4}, Order.C),
                Arguments.of("v2_order_f", new long[]{3, 4}, new int[]{3, 4}, Order.F),
                Arguments.of("v2_order_f_3d", new long[]{2, 3, 4}, new int[]{2, 3, 4}, Order.F),
                Arguments.of("v2_order_f_multichunk", new long[]{5, 7}, new int[]{2, 3}, Order.F)
        );
    }

    /**
     * Every golden fixture must read back as {@code arange}, whatever its {@code order}. Before the
     * {@code order} key was honoured, the F-order 3x4 fixture read as
     * {@code [0, 4, 8, 1, 5, 9, 2, 6, 10, 3, 7, 11]} instead of {@code [0 .. 11]}.
     */
    @ParameterizedTest
    @MethodSource("goldenFixtures")
    public void testReadGoldenFixture(String name, long[] shape, int[] chunks, Order order)
            throws IOException, ZarrException {
        Array array = Array.open(GOLDEN.resolve(name));

        ArrayMetadata metadata = array.metadata();
        Assertions.assertEquals(order, metadata.order, "order was not parsed from .zarray");
        Assertions.assertArrayEquals(shape, metadata.shape);
        Assertions.assertArrayEquals(chunks, metadata.chunks);

        Assertions.assertArrayEquals(expectedArange(shape), readAsLongs(array.read()));
    }

    /**
     * The write direction, checked offline: writing {@code arange} through zarr-java must reproduce
     * the reference implementation's chunk bytes exactly. This is the direction a Java-only
     * round-trip test cannot see. Before the fix, zarr-java wrote {@code "order": "F"} into
     * {@code .zarray} while laying the elements out row-major, so every other implementation
     * misread the result.
     */
    @ParameterizedTest
    @MethodSource("goldenFixtures")
    public void testWriteGoldenFixtureBytes(String name, long[] shape, int[] chunks, Order order)
            throws IOException, ZarrException {
        Path expectedPath = GOLDEN.resolve(name);
        Path actualPath = TESTOUTPUT.resolve("v2_order_write").resolve(name);

        Array array = Array.create(
                new FilesystemStore(actualPath).resolve(),
                Array.metadataBuilder()
                        .withShape(shape)
                        .withChunks(chunks)
                        .withDataType(DataType.INT32)
                        .withOrder(order)
                        .withFillValue(0)
                        .build(),
                true
        );
        array.write(new long[shape.length], arange(shape));

        List<String> expectedChunks = chunkFileNames(expectedPath);
        List<String> actualChunks = chunkFileNames(actualPath);
        Assertions.assertEquals(expectedChunks, actualChunks,
                "zarr-java did not write the same set of chunk files as zarr-python");

        for (String chunk : expectedChunks) {
            Assertions.assertArrayEquals(
                    Files.readAllBytes(expectedPath.resolve(chunk)),
                    Files.readAllBytes(actualPath.resolve(chunk)),
                    "chunk '" + chunk + "' of " + name + " does not match the bytes written by zarr-python"
            );
        }
    }

    /**
     * A read that happens to cover exactly one full chunk takes the fast path in
     * {@link dev.zarr.zarrjava.core.Array#read(long[], long[])}, which returns the decoded chunk
     * straight to the caller instead of copying it into a fresh output array. The decoded chunk
     * therefore has to be physically contiguous in row-major order: a permuted view would answer
     * iterators and {@code get(int[])} correctly while linear accessors such as {@code getInt(int)}
     * silently walked the column-major backing store.
     */
    @Test
    public void testSingleFullChunkReadIsPhysicallyContiguous() throws IOException, ZarrException {
        // The 3x4 fixture has a single 3x4 chunk, so read() returns it through the fast path.
        ucar.ma2.Array result = Array.open(GOLDEN.resolve("v2_order_f")).read();

        for (int i = 0; i < result.getSize(); i++) {
            Assertions.assertEquals(i, result.getInt(i),
                    "linear access at " + i + " walked the column-major backing store");
        }
        Assertions.assertEquals(3, result.getInt(result.getIndex().set(0, 3)),
                "index-based access disagrees with linear access");
    }

    /**
     * C and F order must hold the same logical values but must not produce the same bytes on disk.
     * The second half of that is what actually pins the behaviour down: if {@code order} were
     * ignored again, both stores would come out byte-identical and this would fail.
     */
    @Test
    public void testCAndFOrderAgreeLogicallyButDifferOnDisk() throws IOException, ZarrException {
        long[] shape = {3, 4};
        Path cPath = TESTOUTPUT.resolve("v2_order_compare_c");
        Path fPath = TESTOUTPUT.resolve("v2_order_compare_f");

        Array cArray = writeArange(cPath, shape, new int[]{3, 4}, Order.C);
        Array fArray = writeArange(fPath, shape, new int[]{3, 4}, Order.F);

        Assertions.assertArrayEquals(readAsLongs(cArray.read()), readAsLongs(fArray.read()),
                "C and F order must expose the same logical values");

        byte[] cBytes = Files.readAllBytes(cPath.resolve("0.0"));
        byte[] fBytes = Files.readAllBytes(fPath.resolve("0.0"));
        Assertions.assertFalse(java.util.Arrays.equals(cBytes, fBytes),
                "C and F order produced identical chunk bytes, so 'order' was ignored");
    }

    /**
     * For rank 0 and rank 1 the two orders describe the same layout, so they must produce identical
     * bytes. This guards the {@code ndim() > 1} short-circuit in the v2 codec pipeline.
     */
    @Test
    public void testOrderIsIrrelevantForOneDimension() throws IOException, ZarrException {
        long[] shape = {10};
        Path cPath = TESTOUTPUT.resolve("v2_order_1d_c");
        Path fPath = TESTOUTPUT.resolve("v2_order_1d_f");

        Array cArray = writeArange(cPath, shape, new int[]{4}, Order.C);
        Array fArray = writeArange(fPath, shape, new int[]{4}, Order.F);

        Assertions.assertArrayEquals(expectedArange(shape), readAsLongs(cArray.read()));
        Assertions.assertArrayEquals(expectedArange(shape), readAsLongs(fArray.read()));

        for (String chunk : chunkFileNames(cPath)) {
            Assertions.assertArrayEquals(
                    Files.readAllBytes(cPath.resolve(chunk)),
                    Files.readAllBytes(fPath.resolve(chunk)),
                    "1D arrays must serialize identically in C and F order"
            );
        }
    }

    /**
     * F order has to keep working once a compressor sits behind it in the pipeline, and for a chunk
     * grid that does not divide the shape evenly. The round-trip assertion alone cannot detect a
     * broken {@code order}; it is the pipeline-integration half of this test, checking that the
     * axis reversal is ordered correctly relative to the compressor and survives partial-chunk
     * copying. {@link #testCompressedCAndFOrderDifferOnDisk()} supplies the on-disk check.
     */
    @ParameterizedTest
    @EnumSource(Order.class)
    public void testOrderRoundTripsWithCompressorAndPartialChunks(Order order)
            throws IOException, ZarrException {
        long[] shape = {5, 7, 3};
        Path path = TESTOUTPUT.resolve("v2_order_zlib_" + order);

        Array array = writeCompressedArange(path, shape, order);

        Assertions.assertArrayEquals(expectedArange(shape), readAsLongs(array.read()));

        // A cutout that starts and ends inside a chunk, to check the axis reversal survives
        // partial-chunk copying rather than only whole-chunk reads.
        ucar.ma2.Array cutout = array.read(new long[]{1, 2, 1}, new long[]{3, 4, 2});
        long[] expected = new long[3 * 4 * 2];
        int i = 0;
        for (long z = 1; z < 4; z++) {
            for (long y = 2; y < 6; y++) {
                for (long x = 1; x < 3; x++) {
                    expected[i++] = z * 7 * 3 + y * 3 + x;
                }
            }
        }
        Assertions.assertArrayEquals(expected, readAsLongs(cutout));
    }

    /**
     * The on-disk half of {@link #testOrderRoundTripsWithCompressorAndPartialChunks(Order)}: with a
     * compressor in the pipeline the two orders must still produce different chunk bytes.
     */
    @Test
    public void testCompressedCAndFOrderDifferOnDisk() throws IOException, ZarrException {
        long[] shape = {5, 7, 3};
        Path cPath = TESTOUTPUT.resolve("v2_order_zlib_compare_c");
        Path fPath = TESTOUTPUT.resolve("v2_order_zlib_compare_f");

        writeCompressedArange(cPath, shape, Order.C);
        writeCompressedArange(fPath, shape, Order.F);

        Assertions.assertFalse(
                java.util.Arrays.equals(
                        Files.readAllBytes(cPath.resolve("0.0.0")),
                        Files.readAllBytes(fPath.resolve("0.0.0"))),
                "C and F order produced identical compressed chunk bytes, so 'order' was ignored"
        );
    }

    /**
     * {@code order} must survive the metadata rewrites done by {@code resize} and attribute updates,
     * otherwise an F-order array silently turns into a C-order one whose chunks are now misread.
     */
    @Test
    public void testOrderSurvivesMetadataRewrites() throws IOException, ZarrException {
        Path path = TESTOUTPUT.resolve("v2_order_rewrite");
        Array array = writeArange(path, new long[]{3, 4}, new int[]{3, 4}, Order.F);

        Array resized = array.resize(new long[]{6, 4});
        Assertions.assertEquals(Order.F, resized.metadata().order, "resize dropped the order");

        Array withAttributes = resized.updateAttributes(attributes -> {
            attributes.put("key", "value");
            return attributes;
        });
        Assertions.assertEquals(Order.F, withAttributes.metadata().order,
                "updateAttributes dropped the order");

        // The original data must still be readable through the rewritten metadata.
        ucar.ma2.Array original = withAttributes.read(new long[]{0, 0}, new long[]{3, 4});
        Assertions.assertArrayEquals(expectedArange(new long[]{3, 4}), readAsLongs(original));
    }

    // --- helpers -------------------------------------------------------------------------------

    private static Array writeArange(Path path, long[] shape, int[] chunks, Order order)
            throws IOException, ZarrException {
        Array array = Array.create(
                new FilesystemStore(path).resolve(),
                Array.metadataBuilder()
                        .withShape(shape)
                        .withChunks(chunks)
                        .withDataType(DataType.INT32)
                        .withOrder(order)
                        .withFillValue(0)
                        .build(),
                true
        );
        array.write(new long[shape.length], arange(shape));
        return array;
    }

    private static Array writeCompressedArange(Path path, long[] shape, Order order)
            throws IOException, ZarrException {
        Array array = Array.create(
                new FilesystemStore(path).resolve(),
                Array.metadataBuilder()
                        .withShape(shape)
                        .withChunks(2, 3, 2)
                        .withDataType(DataType.FLOAT64)
                        .withOrder(order)
                        .withFillValue(0)
                        .withZlibCompressor(5)
                        .build(),
                true
        );
        array.write(new long[shape.length], arange(shape, DataType.FLOAT64));
        return array;
    }

    private static ucar.ma2.Array arange(long[] shape) {
        return arange(shape, DataType.INT32);
    }

    private static ucar.ma2.Array arange(long[] shape, DataType dataType) {
        int[] intShape = new int[shape.length];
        for (int i = 0; i < shape.length; i++) {
            intShape[i] = (int) shape[i];
        }
        ucar.ma2.Array array = ucar.ma2.Array.factory(dataType.getMA2DataType(), intShape);
        for (int i = 0; i < array.getSize(); i++) {
            array.setLong(i, i);
        }
        return array;
    }

    private static long[] expectedArange(long[] shape) {
        int size = 1;
        for (long dim : shape) {
            size *= (int) dim;
        }
        long[] expected = new long[size];
        for (int i = 0; i < size; i++) {
            expected[i] = i;
        }
        return expected;
    }

    /**
     * Flattens in row-major index order, independent of the element type, so that a single
     * assertion works for every dtype under test.
     */
    private static long[] readAsLongs(ucar.ma2.Array array) {
        List<Long> values = new ArrayList<>();
        ucar.ma2.IndexIterator iterator = array.getIndexIterator();
        while (iterator.hasNext()) {
            values.add(iterator.getLongNext());
        }
        long[] result = new long[values.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = values.get(i);
        }
        return result;
    }

    private static List<String> chunkFileNames(Path storePath) throws IOException {
        try (Stream<Path> files = Files.list(storePath)) {
            return files.map(path -> path.getFileName().toString())
                    .filter(name -> !name.startsWith("."))
                    .sorted(Comparator.naturalOrder())
                    .collect(Collectors.toList());
        }
    }
}
