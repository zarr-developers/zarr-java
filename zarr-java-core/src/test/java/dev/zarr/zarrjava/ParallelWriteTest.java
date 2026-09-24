package dev.zarr.zarrjava;

import dev.zarr.zarrjava.store.FilesystemStore;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Array;
import dev.zarr.zarrjava.v3.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ParallelWriteTest extends ZarrTest {

    @Test
    public void testParallelWriteDataSafety() throws IOException, ZarrException {
        // Test internal parallelism of write method (using parallel=true)
        Path path = TESTOUTPUT.resolve("parallel_write_safety");
        StoreHandle storeHandle = new FilesystemStore(path).resolve();

        int shape = 1000;
        int chunk = 100;

        Array array = Array.create(storeHandle, Array.metadataBuilder()
                .withShape(shape, shape)
                .withDataType(DataType.INT32)
                .withChunkShape(chunk, chunk)
                .withFillValue(0)
                .build());

        int[] data = new int[shape * shape];
        // Fill with some deterministic pattern
        for (int i = 0; i < shape * shape; i++) {
            data[i] = i;
        }

        ucar.ma2.Array outputData = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, new int[]{shape, shape}, data);

        // Write in parallel
        array.write(outputData, true);

        // Read back
        ucar.ma2.Array readData = array.read();
        int[] readArr = (int[]) readData.get1DJavaArray(ucar.ma2.DataType.INT);

        Assertions.assertArrayEquals(data, readArr, "Data read back should match data written in parallel");
    }

    @Test
    public void testParallelWriteWithSharding() throws IOException, ZarrException {
        // Test internal parallelism with Sharding (nested chunks + shared codec state potential)
        Path path = TESTOUTPUT.resolve("parallel_write_sharding");
        StoreHandle storeHandle = new FilesystemStore(path).resolve();

        int shape = 128; // 128x128
        int shardSize = 64; // Shards are 64x64
        int innerChunk = 32; // Inner chunks 32x32

        // Metadata with sharding
        // With shape 128 and shardSize 64, we have 2x2 = 4 shards.
        // Array.write(parallel=true) will likely process these shards concurrently.
        dev.zarr.zarrjava.v3.ArrayMetadata metadata = Array.metadataBuilder()
                .withShape(shape, shape)
                .withDataType(DataType.INT32)
                .withChunkShape(shardSize, shardSize) // This sets the shard shape (outer chunks)
                .withCodecs(c -> c.withSharding(new int[]{innerChunk, innerChunk}, c2 -> c2.withBytes("LITTLE")))
                .withFillValue(0)
                .build();

        Array array = Array.create(storeHandle, metadata);

        int[] data = new int[shape * shape];
        for (int i = 0; i < shape * shape; i++) {
            data[i] = i;
        }

        ucar.ma2.Array outputData = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, new int[]{shape, shape}, data);

        // Write in parallel
        array.write(outputData, true);

        ucar.ma2.Array readData = array.read();
        int[] readArr = (int[]) readData.get1DJavaArray(ucar.ma2.DataType.INT);

        Assertions.assertArrayEquals(data, readArr, "Sharded data written in parallel should match");
    }

    @Test
    public void testConcurrentWritesDifferentChunks() throws IOException, ZarrException, InterruptedException, ExecutionException {
        // Test external parallelism (multiple threads calling write on same Array instance)
        Path path = TESTOUTPUT.resolve("concurrent_write_safety");
        StoreHandle storeHandle = new FilesystemStore(path).resolve();

        int chunksX = 10;
        int chunksY = 10;
        int chunkSize = 50;
        int shapeX = chunksX * chunkSize;
        int shapeY = chunksY * chunkSize;

        Array array = Array.create(storeHandle, Array.metadataBuilder()
                .withShape(shapeX, shapeY)
                .withDataType(DataType.INT32)
                .withChunkShape(chunkSize, chunkSize)
                .withFillValue(-1)
                .build());

        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int i = 0; i < chunksX; i++) {
            for (int j = 0; j < chunksY; j++) {
                final int cx = i;
                final int cy = j;
                tasks.add(() -> {
                    int[] chunkData = new int[chunkSize * chunkSize];
                    int val = cx * chunksY + cy; // Unique value per chunk
                    java.util.Arrays.fill(chunkData, val);

                    ucar.ma2.Array ucarArray = ucar.ma2.Array.factory(ucar.ma2.DataType.INT, new int[]{chunkSize, chunkSize}, chunkData);

                    // Write to specific chunk offset
                    long[] offset = new long[]{cx * chunkSize, cy * chunkSize};
                    // Use internal parallelism false to isolate external concurrency test mechanism
                    array.write(offset, ucarArray, false);
                    return null;
                });
            }
        }

        List<Future<Void>> futures = executor.invokeAll(tasks);

        for (Future<Void> f : futures) {
            f.get(); // Check for exceptions
        }
        executor.shutdown();

        // Verification
        ucar.ma2.Array readData = array.read();
        for (int i = 0; i < chunksX; i++) {
            for (int j = 0; j < chunksY; j++) {
                int expectedVal = i * chunksY + j;
                int originX = i * chunkSize;
                int originY = j * chunkSize;

                // Verify a pixel in the chunk
                int val = readData.getInt(readData.getIndex().set(originX, originY));
                Assertions.assertEquals(expectedVal, val, "Value at chunk " + i + "," + j + " mismatch");
            }
        }
    }
}
