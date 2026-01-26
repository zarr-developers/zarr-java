package dev.zarr.zarrjava.v2;

import com.scalableminds.bloscjava.Blosc;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;
import dev.zarr.zarrjava.v2.codec.Codec;
import dev.zarr.zarrjava.v2.codec.core.BloscCodec;
import dev.zarr.zarrjava.v2.codec.core.ZlibCodec;

public class ArrayMetadataBuilder {
    long[] shape = null;
    int[] chunks = null;
    DataType dataType = null;
    Order order = Order.C;
    Separator dimensionSeparator = Separator.DOT;
    Object fillValue = null;
    Codec[] filters = null;
    Codec compressor = null;
    Attributes attributes = new Attributes();


    protected ArrayMetadataBuilder() {
    }

    protected static ArrayMetadataBuilder fromArrayMetadata(ArrayMetadata arrayMetadata) {
        return fromArrayMetadata(arrayMetadata, true);
    }

    protected static ArrayMetadataBuilder fromArrayMetadata(ArrayMetadata arrayMetadata, boolean withAttributes) {
        ArrayMetadataBuilder builder = new ArrayMetadataBuilder();
        builder.shape = arrayMetadata.shape;
        builder.chunks = arrayMetadata.chunks;
        builder.dataType = arrayMetadata.dataType;
        builder.order = arrayMetadata.order;
        builder.dimensionSeparator = arrayMetadata.dimensionSeparator;
        builder.fillValue = arrayMetadata.parsedFillValue;
        builder.filters = arrayMetadata.filters;
        builder.compressor = arrayMetadata.compressor;
        if (withAttributes) {
            builder.attributes = arrayMetadata.attributes;
        }
        return builder;
    }

    public ArrayMetadataBuilder withShape(long... shape) {
        this.shape = shape;
        return this;
    }

    public ArrayMetadataBuilder withChunks(int... chunks) {
        this.chunks = chunks;
        return this;
    }

    public ArrayMetadataBuilder withDataType(DataType dataTypeV2) {
        this.dataType = dataTypeV2;
        return this;
    }

    public ArrayMetadataBuilder withOrder(Order order) {
        this.order = order;
        return this;
    }

    public ArrayMetadataBuilder withDimensionSeparator(Separator dimensionSeparator) {
        this.dimensionSeparator = dimensionSeparator;
        return this;
    }

    public ArrayMetadataBuilder withFillValue(Object fillValue) {
        this.fillValue = fillValue;
        return this;
    }

    public ArrayMetadataBuilder withCompressor(Codec compressor) {
        this.compressor = compressor;
        return this;
    }

    public ArrayMetadataBuilder withBloscCompressor(
            Blosc.Compressor cname, Blosc.Shuffle shuffle, int clevel, int typeSize,
            int blockSize
    ) {
        try {
            this.compressor = new BloscCodec(cname, shuffle, clevel, typeSize, blockSize);
        } catch (ZarrException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public ArrayMetadataBuilder withBloscCompressor(String cname, String shuffle, int clevel, int blockSize) {
        if (shuffle.equals("shuffle")) {
            shuffle = "byteshuffle";
        }
        return withBloscCompressor(Blosc.Compressor.fromString(cname), Blosc.Shuffle.fromString(shuffle), clevel,
                dataType.getByteCount(), blockSize
        );
    }

    public ArrayMetadataBuilder withBloscCompressor(String cname, String shuffle, int clevel) {
        return withBloscCompressor(cname, shuffle, clevel, 0);
    }

    public ArrayMetadataBuilder withBloscCompressor(String cname, int clevel) {
        return withBloscCompressor(cname, "noshuffle", clevel);
    }

    public ArrayMetadataBuilder withBloscCompressor(String cname) {
        return withBloscCompressor(cname, 5);
    }

    public ArrayMetadataBuilder withBloscCompressor() {
        return withBloscCompressor("zstd");
    }

    public ArrayMetadataBuilder withZlibCompressor(int level) {
        try {
            this.compressor = new ZlibCodec(level);
        } catch (ZarrException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public ArrayMetadataBuilder withZlibCompressor() {
        return withZlibCompressor(5);
    }

    public ArrayMetadataBuilder putAttribute(String key, Object value) {
        this.attributes.put(key, value);
        return this;
    }

    public ArrayMetadataBuilder withAttributes(Attributes attributes) {
        if (this.attributes == null) {
            this.attributes = attributes;
        } else {
            this.attributes.putAll(attributes);
        }
        return this;
    }

    public ArrayMetadata build() throws ZarrException {
        if (shape == null) {
            throw new IllegalStateException("Please call `withShape` first.");
        }
        if (dataType == null) {
            throw new IllegalStateException("Please call `withDataType` first.");
        }
        
        // If chunks are not specified, calculate default chunks
        if (chunks == null) {
            chunks = calculateDefaultChunks(shape);
        }
        
        return new ArrayMetadata(
                2,
                shape,
                chunks,
                dataType,
                fillValue,
                order,
                filters,
                compressor,
                dimensionSeparator,
                attributes
        );
    }
    
    /**
     * Calculate default chunk shape when not specified.
     * This implements JZarr's ArrayParams.build() logic, targeting chunks of approximately 512 elements.
     * 
     * The algorithm divides each dimension by 512 to determine the number of ~512-sized chunks,
     * then calculates chunk sizes that will cover the dimension. Note that the total coverage
     * may slightly exceed the dimension size (e.g., for shape=1024, chunks=342 results in 
     * 3 chunks covering 1026 elements). This is intentional and matches JZarr behavior - 
     * Zarr handles out-of-bounds gracefully, and the goal is approximate chunk sizes rather 
     * than perfect tiling.
     */
    private int[] calculateDefaultChunks(long[] shape) {
        int[] chunks = new int[shape.length];
        for (int i = 0; i < shape.length; i++) {
            long shapeDim = shape[i];
            int numChunks = (int) (shapeDim / 512);
            if (numChunks > 0) {
                int chunkDim = (int) (shapeDim / (numChunks + 1));
                if (shapeDim % chunkDim == 0) {
                    chunks[i] = chunkDim;
                } else {
                    chunks[i] = chunkDim + 1;
                }
            } else {
                // If dimension is smaller than 512, use the full dimension
                chunks[i] = (int) shapeDim;
            }
        }
        return chunks;
    }
}