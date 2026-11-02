package dev.zarr.zarrjava;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static dev.zarr.zarrjava.core.Node.ZARR_JSON;

public class ArrayBoundsTest extends ZarrTest {

    static final ObjectMapper MAPPER = new ObjectMapper();

    static Array createExample(Path path) throws Exception {
        Array array = Array.create(new FilesystemStore(path).resolve(),
                Array.metadataBuilder()
                        .withShape(6)
                        .withDataType(DataType.INT32)
                        .withChunkShape(4)
                        .withFillValue(0)
                        .build());
        array.write(ucar.ma2.Array.makeFromJavaArray(new int[]{10, 11, 12, 13, 14, 15}));
        return array;
    }

    static JsonNode readZarrJson(Path path) throws IOException {
        return MAPPER.readTree(path.resolve(ZARR_JSON).toFile());
    }

    @Test
    public void testPrependKeepsExistingChunks() throws Exception {
        Path path = TESTOUTPUT.resolve("bounds_prepend");
        Array array = createExample(path);
        byte[] chunk0 = Files.readAllBytes(path.resolve("c/0"));
        byte[] chunk1 = Files.readAllBytes(path.resolve("c/1"));

        array = array.prepend(ucar.ma2.Array.makeFromJavaArray(new int[]{7, 8, 9}));

        Assertions.assertArrayEquals(new long[]{9}, array.metadata().shape);
        Assertions.assertArrayEquals(new long[]{-3}, array.metadata().lowerBound());
        Assertions.assertArrayEquals(new int[]{7, 8, 9, 10, 11, 12, 13, 14, 15},
                (int[]) array.read().get1DJavaArray(ucar.ma2.DataType.INT));

        Assertions.assertArrayEquals(chunk0, Files.readAllBytes(path.resolve("c/0")));
        Assertions.assertArrayEquals(chunk1, Files.readAllBytes(path.resolve("c/1")));
        Assertions.assertArrayEquals(new int[]{0, 7, 8, 9},
                (int[]) array.readChunk(new long[]{-1}).get1DJavaArray(ucar.ma2.DataType.INT));

        JsonNode zarrJson = readZarrJson(path);
        Assertions.assertFalse(zarrJson.has("shape"));
        Assertions.assertEquals("{\"inclusive_min\":[-3],\"exclusive_max\":[6]}",
                zarrJson.get("bounds").toString());

        Array reopened = Array.open(path);
        Assertions.assertArrayEquals(new long[]{-3}, reopened.metadata().lowerBound());
        Assertions.assertArrayEquals(new int[]{8, 9, 10},
                (int[]) reopened.read(new long[]{1}, new long[]{3}).get1DJavaArray(ucar.ma2.DataType.INT));
    }

    @Test
    public void testPrependAcrossSeveralChunksAndAxes() throws Exception {
        Path path = TESTOUTPUT.resolve("bounds_prepend_2d");
        Array array = Array.create(new FilesystemStore(path).resolve(),
                Array.metadataBuilder()
                        .withShape(4, 3)
                        .withDataType(DataType.INT32)
                        .withChunkShape(2, 2)
                        .withFillValue(-1)
                        .build());
        ucar.ma2.Array original = arange(new int[]{4, 3}, 0);
        array.write(original);

        ucar.ma2.Array rows = arange(new int[]{5, 3}, 100);
        array = array.prepend(rows, 0);
        ucar.ma2.Array cols = arange(new int[]{9, 1}, 200);
        array = array.prepend(cols, 1);

        Assertions.assertArrayEquals(new long[]{-5, -1}, array.metadata().lowerBound());
        Assertions.assertArrayEquals(new long[]{9, 4}, array.metadata().shape);
        ucar.ma2.Array result = array.read();
        for (int i = 0; i < 9; i++) {
            Assertions.assertEquals(200 + i, result.getInt(result.getIndex().set(i, 0)));
            for (int j = 0; j < 3; j++) {
                int expected = i < 5 ? 100 + i * 3 + j : (i - 5) * 3 + j;
                Assertions.assertEquals(expected, result.getInt(result.getIndex().set(i, j + 1)));
            }
        }
        Assertions.assertTrue(Files.exists(path.resolve("c/-3/-1")));
        Assertions.assertTrue(Files.exists(path.resolve("c/1/1")));
    }

    @ParameterizedTest
    @ValueSource(longs = {-9, -7, -4, -3, -1, 0, 2, 5})
    public void testReadWriteMatchesReference(long lowerBound) throws Exception {
        StoreHandle store = new FilesystemStore(TESTOUTPUT).resolve("bounds_reference_" + lowerBound);
        Array array = Array.create(store,
                Array.metadataBuilder()
                        .withBounds(new long[]{lowerBound, -lowerBound}, new long[]{lowerBound + 11, -lowerBound + 7})
                        .withDataType(DataType.INT32)
                        .withChunkShape(4, 3)
                        .withFillValue(0)
                        .build());
        ucar.ma2.Array reference = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, new int[]{11, 7});
        Random random = new Random(lowerBound);
        for (int n = 0; n < 20; n++) {
            int[] offset = {random.nextInt(11), random.nextInt(7)};
            int[] shape = {1 + random.nextInt(11 - offset[0]), 1 + random.nextInt(7 - offset[1])};
            ucar.ma2.Array data = arange(shape, 1 + n * 100);
            array.write(new long[]{offset[0], offset[1]}, data);
            MultiArrayUtils.copyRegion(data, new int[2], reference, offset, shape);

            int[] readOffset = {random.nextInt(11), random.nextInt(7)};
            int[] readShape = {1 + random.nextInt(11 - readOffset[0]), 1 + random.nextInt(7 - readOffset[1])};
            Assertions.assertTrue(MultiArrayUtils.allValuesEqual(
                    reference.sectionNoReduce(readOffset, readShape, null),
                    array.read(new long[]{readOffset[0], readOffset[1]}, new long[]{readShape[0], readShape[1]})));
        }
        Assertions.assertTrue(MultiArrayUtils.allValuesEqual(reference, Array.open(store).read()));
    }

    @Test
    public void testPrependWithSharding() throws Exception {
        Path path = TESTOUTPUT.resolve("bounds_sharding");
        Array array = Array.create(new FilesystemStore(path).resolve(),
                Array.metadataBuilder()
                        .withShape(8, 8)
                        .withDataType(DataType.INT32)
                        .withChunkShape(4, 4)
                        .withFillValue(0)
                        .withCodecs(c -> c.withSharding(new int[]{2, 2}))
                        .build());
        array.write(arange(new int[]{8, 8}, 1));
        byte[] shard = Files.readAllBytes(path.resolve("c/0/0"));

        array = array.prepend(arange(new int[]{3, 8}, 1000), 0);

        Assertions.assertArrayEquals(shard, Files.readAllBytes(path.resolve("c/0/0")));
        ucar.ma2.Array result = Array.open(path).read();
        Assertions.assertEquals(1000, result.getInt(result.getIndex().set(0, 0)));
        Assertions.assertEquals(1, result.getInt(result.getIndex().set(3, 0)));
        Assertions.assertEquals(64, result.getInt(result.getIndex().set(10, 7)));
    }

    @Test
    public void testResizeKeepsLowerBound() throws Exception {
        Path path = TESTOUTPUT.resolve("bounds_resize");
        Array array = createExample(path).prepend(ucar.ma2.Array.makeFromJavaArray(new int[]{7, 8, 9}));

        array = array.resize(new long[]{5}, false);
        Assertions.assertArrayEquals(new long[]{-3}, array.metadata().lowerBound());
        Assertions.assertFalse(Files.exists(path.resolve("c/1")));
        Assertions.assertArrayEquals(new int[]{7, 8, 9, 10, 11},
                (int[]) array.read().get1DJavaArray(ucar.ma2.DataType.INT));

        array = array.resize(new long[]{9});
        Assertions.assertArrayEquals(new int[]{7, 8, 9, 10, 11, 0, 0, 0, 0},
                (int[]) array.read().get1DJavaArray(ucar.ma2.DataType.INT));
    }

    @Test
    public void testZeroLowerBoundIsWrittenAsShape() throws Exception {
        Path path = TESTOUTPUT.resolve("bounds_zero");
        writeZarrJson(path, "\"bounds\": {\"inclusive_min\": [0, 0], \"exclusive_max\": [6, 7]}");
        Array array = Array.open(path);
        Assertions.assertArrayEquals(new long[]{6, 7}, array.metadata().shape);

        array.setAttributes(new dev.zarr.zarrjava.core.Attributes());
        JsonNode zarrJson = readZarrJson(path);
        Assertions.assertEquals("[6,7]", zarrJson.get("shape").toString());
        Assertions.assertFalse(zarrJson.has("bounds"));
    }

    @Test
    public void testReadExampleFromZep() throws Exception {
        Path path = TESTOUTPUT.resolve("bounds_zep_example");
        writeZarrJson(path, "\"bounds\": {\"inclusive_min\": [-3, -2], \"exclusive_max\": [6, 7]}");
        Array array = Array.open(path);
        Assertions.assertArrayEquals(new long[]{9, 9}, array.metadata().shape);

        array.write(arange(new int[]{9, 9}, 1));
        for (String key : new String[]{"c/-1/-1", "c/-1/0", "c/-1/1", "c/0/-1", "c/0/0", "c/0/1", "c/1/-1", "c/1/0", "c/1/1"}) {
            Assertions.assertTrue(Files.exists(path.resolve(key)), key);
        }
        // the first element is at position 1 of chunk -1 along dim 0 and position 3 of chunk -1 along dim 1
        ucar.ma2.Array chunk = array.readChunk(new long[]{-1, -1});
        Assertions.assertEquals(1, chunk.getInt(chunk.getIndex().set(1, 3)));
        Assertions.assertEquals(2, chunk.getInt(chunk.getIndex().set(1, 4)));
        Assertions.assertEquals(0, chunk.getInt(chunk.getIndex().set(0, 0)));
    }

    @Test
    public void testInvalidMetadata() throws Exception {
        String bounds = "\"bounds\": {\"inclusive_min\": [-3], \"exclusive_max\": [6]}";
        writeZarrJson(TESTOUTPUT.resolve("bounds_valid"), bounds, "[4]");
        Assertions.assertArrayEquals(new long[]{9}, Array.open(TESTOUTPUT.resolve("bounds_valid")).metadata().shape);
        assertOpenFails("bounds_and_shape", bounds + ", \"shape\": [9]");
        assertOpenFails("bounds_none", null);
        assertOpenFails("bounds_inverted", "\"bounds\": {\"inclusive_min\": [6], \"exclusive_max\": [-3]}");
        assertOpenFails("bounds_length", "\"bounds\": {\"inclusive_min\": [0, 0], \"exclusive_max\": [6]}");
        assertOpenFails("bounds_missing_max", "\"bounds\": {\"inclusive_min\": [0]}");
    }

    @Test
    public void testOriginIsRejected() throws Exception {
        Path path = TESTOUTPUT.resolve("bounds_origin");
        Files.createDirectories(path);
        Files.write(path.resolve(ZARR_JSON), ("{\"zarr_format\": 3, \"node_type\": \"array\", " +
                "\"bounds\": {\"inclusive_min\": [-3], \"exclusive_max\": [6]}, \"data_type\": \"int32\", " +
                "\"chunk_grid\": {\"name\": \"regular\", \"configuration\": {\"chunk_shape\": [4], \"origin\": [1]}}, " +
                "\"chunk_key_encoding\": {\"name\": \"default\"}, " +
                "\"codecs\": [{\"name\": \"bytes\", \"configuration\": {\"endian\": \"little\"}}], " +
                "\"fill_value\": 0}").getBytes(StandardCharsets.UTF_8));
        Assertions.assertThrows(Exception.class, () -> Array.open(path));
    }

    void assertOpenFails(String name, String extent) throws IOException {
        Path path = TESTOUTPUT.resolve(name);
        writeZarrJson(path, extent, "[4]");
        Assertions.assertThrows(Exception.class, () -> Array.open(path));
    }

    static void writeZarrJson(Path path, String extent) throws IOException {
        writeZarrJson(path, extent, "[4, 5]");
    }

    static void writeZarrJson(Path path, String extent, String chunkShape) throws IOException {
        Files.createDirectories(path);
        Files.write(path.resolve(ZARR_JSON), ("{\"zarr_format\": 3, \"node_type\": \"array\", " +
                (extent == null ? "" : extent + ", ") +
                "\"data_type\": \"int32\", " +
                "\"chunk_grid\": {\"name\": \"regular\", \"configuration\": {\"chunk_shape\": " + chunkShape + "}}, " +
                "\"chunk_key_encoding\": {\"name\": \"default\"}, " +
                "\"codecs\": [{\"name\": \"bytes\", \"configuration\": {\"endian\": \"little\"}}], " +
                "\"fill_value\": 0}").getBytes(StandardCharsets.UTF_8));
    }

    static ucar.ma2.Array arange(int[] shape, int start) {
        ucar.ma2.Array array = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, shape);
        for (int i = 0; i < array.getSize(); i++) {
            array.setInt(i, start + i);
        }
        return array;
    }
}
