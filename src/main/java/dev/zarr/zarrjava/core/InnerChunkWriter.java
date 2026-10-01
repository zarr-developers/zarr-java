package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.ArrayBytesCodec.WithPartialDecode.InnerChunkUpdate;
import dev.zarr.zarrjava.store.StoreHandle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes already-encoded inner chunks into a Zarr array without decoding or re-encoding anything,
 * batching the changes so that each affected shard is rebuilt exactly once.
 * <p>
 * Inner chunks are staged with {@link #put(long[], ByteBuffer)} and applied by {@link #flush()}. A
 * shard holds an index plus the independently compressed bytes of its inner chunks, and stores offer
 * no partial writes, so replacing one inner chunk means reading the shard, splicing the new bytes in,
 * and storing the whole shard again. Every other inner chunk is copied through byte for byte, still
 * encoded; only the shard index is ever decoded. Compare
 * {@link Array#writeChunk(long[], ucar.ma2.Array)}, which for a sharded array decodes the shard and
 * recompresses every inner chunk in it.
 * <p>
 * Batching is what makes this cheap: staging several inner chunks that fall into the same shard and
 * then flushing once rebuilds that shard once, not once per inner chunk.
 * <p>
 * <b>Unsafe:</b> because nothing is decoded, nothing can be validated. The caller must guarantee that
 * each buffer holds exactly one inner chunk of shape {@link Array#innerChunkShape()}, encoded exactly
 * as this array's inner codec pipeline would produce it (matching data type, endianness, compressor
 * and options). Incompatible bytes are stored happily and silently corrupt the array; the damage
 * surfaces only when something reads it. {@link Array#readInnerChunkDirect(long[])} on an array with
 * identical metadata is the one source of such bytes that is safe by construction.
 * <p>
 * <b>Whole shards are rewritten.</b> Two writers that touch the same shard concurrently each read the
 * shard, splice their own change in and store it, so the later store silently discards the earlier
 * writer's inner chunk. Callers must serialize writes per shard. Storing a shard is also not
 * crash-atomic in every store implementation, so an interrupted flush can leave a truncated shard,
 * losing inner chunks that were not being changed.
 * <p>
 * Instances are <b>not thread-safe</b>.
 */
public final class InnerChunkWriter {

    @Nonnull
    private final Array array;
    /**
     * The staged inner chunks, grouped by the stored chunk holding them.
     */
    private final Map<String, ChunkBatch> pending = new LinkedHashMap<>();

    public InnerChunkWriter(@Nonnull Array array) {
        this.array = array;
    }

    /**
     * Stages the already-encoded bytes of one inner chunk to be stored on the next {@link #flush()}.
     * Nothing is read from or written to the store here.
     * <p>
     * Only the {@code remaining()} bytes from the buffer's current position are used and the buffer's
     * position is not advanced, but the bytes are <b>not copied</b>: do not modify the buffer's
     * contents before {@link #flush()} returns.
     * <p>
     * Staging the same coordinates twice replaces the earlier entry.
     *
     * @param innerChunkCoords The coordinates of the inner chunk on the grid given by
     *                         {@link Array#innerChunkShape()}, spanning the whole array. This is the
     *                         same coordinate space {@link Array#readInnerChunkDirect(long[])} uses.
     * @param innerChunkBytes  The already-encoded inner chunk bytes, or {@code null} to remove the
     *                         inner chunk (a subsequent read then returns the fill value).
     * @return this writer, so that calls can be chained
     * @throws IllegalArgumentException if {@code innerChunkCoords} has the wrong rank
     * @throws ZarrException            if the inner chunk is outside the array's domain, or if
     *                                  {@code innerChunkBytes} has no remaining bytes
     */
    public InnerChunkWriter put(long[] innerChunkCoords, @Nullable ByteBuffer innerChunkBytes)
            throws ZarrException {
        final long[][] splitCoords = array.splitInnerChunkCoords(innerChunkCoords);
        final long[] chunkCoords = splitCoords[0];
        final long[] coordsInChunk = splitCoords[1];

        if (innerChunkBytes != null && !innerChunkBytes.hasRemaining()) {
            throw new ZarrException(
                    "The encoded bytes for inner chunk " + Arrays.toString(innerChunkCoords) + " are empty. "
                            + "Pass 'null' to remove the inner chunk instead.");
        }
        // duplicate() to snapshot position and limit without ever consuming the caller's buffer
        final ByteBuffer stagedBytes = innerChunkBytes == null ? null : innerChunkBytes.duplicate();

        final String[] chunkKeys = array.metadata().chunkKeyEncoding().encodeChunkKey(chunkCoords);
        pending.computeIfAbsent(String.join("/", chunkKeys), key -> new ChunkBatch(chunkCoords))
                .updates.put(Arrays.toString(coordsInChunk),
                        new InnerChunkUpdate(coordsInChunk, stagedBytes));
        return this;
    }

    /**
     * Stores everything staged since the last flush, rebuilding each affected shard exactly once, and
     * clears the staged inner chunks. A stored chunk that would hold no inner chunks at all is deleted.
     * Flushing with nothing staged is a no-op, and the writer is reusable afterwards.
     * <p>
     * Stored chunks are processed one at a time and each is dropped from the staged set only once its
     * write has succeeded, so a failed flush can be retried: rebuilding a shard twice from the same
     * staged bytes produces byte-identical output.
     *
     * @throws ZarrException if an existing shard cannot be parsed, or if a rebuilt shard would be too
     *                       large to address
     */
    public void flush() throws ZarrException {
        final ArrayMetadata metadata = array.metadata();
        final boolean addressesInnerChunks = array.codecPipeline.supportsPartialDecode();
        final long innerChunksPerChunk = innerChunksPerChunk(metadata);

        final Iterator<ChunkBatch> batches = pending.values().iterator();
        while (batches.hasNext()) {
            final ChunkBatch batch = batches.next();
            final String[] chunkKeys = metadata.chunkKeyEncoding().encodeChunkKey(batch.chunkCoords);
            final StoreHandle chunkHandle = array.storeHandle.resolve(chunkKeys);

            if (!addressesInnerChunks) {
                // The inner chunk grid is the stored chunk grid, so the staged bytes are the whole
                // stored chunk. There is exactly one staged inner chunk per stored chunk here.
                final InnerChunkUpdate update = batch.updates.values().iterator().next();
                if (update.innerChunkBytes == null) {
                    chunkHandle.delete();
                } else {
                    chunkHandle.set(update.innerChunkBytes.duplicate());
                }
                batches.remove();
                continue;
            }

            // Nothing of the existing shard survives when every inner chunk of it is being replaced, so
            // in that case the shard does not need to be read at all.
            final boolean replacesWholeChunk = batch.updates.size() == innerChunksPerChunk
                    && batch.updates.values().stream().allMatch(u -> u.innerChunkBytes != null);
            final ByteBuffer oldChunkBytes = replacesWholeChunk ? null : chunkHandle.read();

            final ByteBuffer newChunkBytes = array.codecPipeline.mergeInnerChunksEncoded(
                    oldChunkBytes, new ArrayList<>(batch.updates.values()));

            if (newChunkBytes == null) {
                chunkHandle.delete();
            } else {
                chunkHandle.set(newChunkBytes);
            }
            batches.remove();
        }
    }

    /**
     * How many inner chunks fit into one stored chunk.
     */
    private long innerChunksPerChunk(ArrayMetadata metadata) {
        final int[] chunkShape = metadata.chunkShape();
        final int[] innerChunkShape = array.innerChunkShape();
        long innerChunksPerChunk = 1;
        for (int dimIdx = 0; dimIdx < chunkShape.length; dimIdx++) {
            innerChunksPerChunk *= chunkShape[dimIdx] / innerChunkShape[dimIdx];
        }
        return innerChunksPerChunk;
    }

    /**
     * The staged inner chunks of one stored chunk, keyed by their coordinates within it so that
     * staging the same inner chunk twice replaces the earlier entry.
     */
    private static final class ChunkBatch {

        final long[] chunkCoords;
        final Map<String, InnerChunkUpdate> updates = new LinkedHashMap<>();

        ChunkBatch(long[] chunkCoords) {
            this.chunkCoords = chunkCoords;
        }
    }
}
