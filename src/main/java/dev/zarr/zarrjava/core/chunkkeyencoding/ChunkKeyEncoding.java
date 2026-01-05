package dev.zarr.zarrjava.core.chunkkeyencoding;

public interface ChunkKeyEncoding {

    String[] encodeChunkKey(long[] chunkCoords);

}
