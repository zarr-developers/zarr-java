package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.codec.ArrayBytesCodec.WithPartialDecode.InnerChunkUpdate;
import dev.zarr.zarrjava.store.StoreHandle;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes already-encoded inner chunks into one shard of a Zarr array without decoding or re-encoding
 * anything, rebuilding the shard once for all staged inner chunks.
 * <p>
 * A writer is scoped to a single shard, given when it is created (see
 * {@link Array#writeInnerChunkDirect(long[])}). Inner chunks are staged with
 * {@link #put(long[], ByteBuffer)} and applied by {@link #flush()}, or by {@link #close()} at the end of
 * a try-with-resources block:
 * <pre>{@code
 * try (InnerChunkWriter writer = array.writeInnerChunkDirect(new long[]{1, 0})) {
 *     writer.put(new long[]{1, 1}, innerChunkBytes);
 *     writer.put(new long[]{0, 0}, otherInnerChunkBytes);
 * }
 * }</pre>
 * A shard holds an index plus the independently compressed bytes of its inner chunks, and stores offer
 * no partial writes, so replacing one inner chunk means reading the shard, splicing the new bytes in,
 * and storing the whole shard again. Every other inner chunk is copied through byte for byte, still
 * encoded; only the shard index is ever decoded. Compare
 * {@link Array#writeChunk(long[], ucar.ma2.Array)}, which for a sharded array decodes the shard and
 * recompresses every inner chunk in it.
 * <p>
 * <b>Unsafe:</b> because nothing is decoded, nothing can be validated. The caller must guarantee that
 * each buffer holds exactly one inner chunk of shape {@link Array#innerChunkShape()}, encoded exactly
 * as this array's inner codec pipeline would produce it (matching data type, endianness, compressor
 * and options). Incompatible bytes are stored happily and silently corrupt the array; the damage
 * surfaces only when something reads it. {@link Array#readInnerChunkDirect(long[]...)} on an array
 * with identical metadata is the one source of such bytes that is safe by construction.
 * <p>
 * <b>Whole shards are rewritten.</b> Two writers for the same shard each read the shard, splice their
 * own change in and store it, so the later store silently discards the earlier writer's inner chunk.
 * Callers must serialize writes per shard. Storing a shard is also not crash-atomic in every store
 * implementation, so an interrupted flush can leave a truncated shard, losing inner chunks that were
 * not being changed.
 * <p>
 * Instances are <b>not thread-safe</b>.
 */
public final class InnerChunkWriter implements AutoCloseable {

    @Nonnull
    private final Array array;
    /**
     * The coordinates of the shard this writer writes into, on the array's chunk grid.
     */
    @Nonnull
    private final long[] shardCoords;
    /**
     * The staged inner chunks, keyed by their coordinates within the shard so that staging the same
     * inner chunk twice replaces the earlier entry.
     */
    private final Map<String, InnerChunkUpdate> pending = new LinkedHashMap<>();
    private boolean closed = false;

    /**
     * @param array       the array to write into
     * @param shardCoords the coordinates of the shard on the array's chunk grid
     * @throws IllegalArgumentException if {@code shardCoords} has the wrong rank
     * @throws ZarrException            if the shard is outside the array's domain
     */
    public InnerChunkWriter(@Nonnull Array array, @Nonnull long[] shardCoords) throws ZarrException {
        final int ndim = array.metadata().ndim();
        if (shardCoords.length != ndim) {
            throw new IllegalArgumentException("'shardCoords' needs to have rank '" + ndim + "'.");
        }
        if (!array.chunkIsInArray(shardCoords)) {
            throw new ZarrException("Attempting to write data outside of the array's domain.");
        }
        this.array = array;
        this.shardCoords = shardCoords.clone();
    }

    /**
     * Stages the already-encoded bytes of one inner chunk of a shard with a single sharding level.
     * Equivalent to {@code put(new long[][]{innerChunkCoords}, innerChunkBytes)}.
     *
     * @see #put(long[][], ByteBuffer)
     */
    public InnerChunkWriter put(long[] innerChunkCoords, @Nullable ByteBuffer innerChunkBytes)
            throws ZarrException {
        return put(new long[][]{innerChunkCoords}, innerChunkBytes);
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
     * @param innerChunkCoords The coordinates of the inner chunk within this writer's shard, one entry
     *                         per sharding level: the inner chunk within the shard, then, if shards are
     *                         nested, the inner chunk within that, and so on. These are the entries
     *                         that follow the shard coordinates in
     *                         {@link Array#readInnerChunkDirect(long[]...)}.
     * @param innerChunkBytes  The already-encoded inner chunk bytes, or {@code null} to remove the
     *                         inner chunk (a subsequent read then returns the fill value).
     * @return this writer, so that calls can be chained
     * @throws IllegalArgumentException if the number of entries does not match the sharding depth, or
     *                                  an entry has the wrong rank
     * @throws IllegalStateException    if this writer has been closed
     * @throws ZarrException            if the inner chunk is outside the shard or the array's domain,
     *                                  or if {@code innerChunkBytes} has no remaining bytes
     */
    public InnerChunkWriter put(long[][] innerChunkCoords, @Nullable ByteBuffer innerChunkBytes)
            throws ZarrException {
        if (closed) {
            throw new IllegalStateException("This InnerChunkWriter has been closed.");
        }
        // Copy so that later changes to the caller's arrays cannot move a staged inner chunk.
        final long[][] coords = new long[innerChunkCoords.length][];
        for (int level = 0; level < innerChunkCoords.length; level++) {
            coords[level] = innerChunkCoords[level] == null ? null : innerChunkCoords[level].clone();
        }
        final long[][] fullCoords = new long[coords.length + 1][];
        fullCoords[0] = shardCoords;
        System.arraycopy(coords, 0, fullCoords, 1, coords.length);
        array.checkInnerChunkCoords(fullCoords);

        if (innerChunkBytes != null && !innerChunkBytes.hasRemaining()) {
            throw new ZarrException(
                    "The encoded bytes for inner chunk " + Arrays.deepToString(fullCoords) + " are empty. "
                            + "Pass 'null' to remove the inner chunk instead.");
        }
        // duplicate() to snapshot position and limit without ever consuming the caller's buffer
        final ByteBuffer stagedBytes = innerChunkBytes == null ? null : innerChunkBytes.duplicate();

        pending.put(Arrays.deepToString(coords), new InnerChunkUpdate(coords, stagedBytes));
        return this;
    }

    /**
     * Stores everything staged since the last flush, rebuilding the shard once, and clears the staged
     * inner chunks. A shard that would hold no inner chunks at all is deleted. Flushing with nothing
     * staged is a no-op, and the writer is reusable afterwards.
     * <p>
     * The staged inner chunks are only cleared once the write has succeeded, so a failed flush can be
     * retried: rebuilding a shard twice from the same staged bytes produces byte-identical output.
     *
     * @throws ZarrException if the existing shard cannot be parsed, or if the rebuilt shard would be
     *                       too large to address
     */
    public void flush() throws ZarrException {
        if (pending.isEmpty()) {
            return;
        }
        final String[] chunkKeys = array.metadata().chunkKeyEncoding().encodeChunkKey(shardCoords);
        final StoreHandle chunkHandle = array.storeHandle.resolve(chunkKeys);

        final ByteBuffer newChunkBytes;
        if (!array.codecPipeline.supportsPartialDecode()) {
            // Without sharding there is nothing to address inside the stored chunk, so the one staged
            // inner chunk is the whole stored chunk.
            newChunkBytes = pending.values().iterator().next().innerChunkBytes;
        } else {
            // Nothing of the existing shard survives when every inner chunk of it is being replaced, so
            // in that case the shard does not need to be read at all.
            final boolean replacesWholeShard = pending.size() == innerChunksPerShard()
                    && pending.values().stream().allMatch(u -> u.innerChunkBytes != null);
            final ByteBuffer oldChunkBytes = replacesWholeShard ? null : chunkHandle.read();
            newChunkBytes = array.codecPipeline.mergeInnerChunksDirect(
                    oldChunkBytes, new ArrayList<>(pending.values()));
        }

        if (newChunkBytes == null) {
            chunkHandle.delete();
        } else {
            chunkHandle.set(newChunkBytes.duplicate());
        }
        pending.clear();
    }

    /**
     * Flushes the staged inner chunks and closes this writer; further {@link #put} calls fail. If the
     * flush fails the writer stays open, so that closing it again retries the flush.
     *
     * @throws ZarrException if flushing fails
     */
    @Override
    public void close() throws ZarrException {
        if (closed) {
            return;
        }
        flush();
        closed = true;
    }

    /**
     * How many innermost inner chunks fit into one shard.
     */
    private long innerChunksPerShard() {
        final int[] chunkShape = array.metadata().chunkShape();
        final int[] innerChunkShape = array.codecPipeline.innerChunkShape();
        long innerChunksPerShard = 1;
        for (int dimIdx = 0; dimIdx < chunkShape.length; dimIdx++) {
            innerChunksPerShard *= chunkShape[dimIdx] / innerChunkShape[dimIdx];
        }
        return innerChunksPerShard;
    }
}
