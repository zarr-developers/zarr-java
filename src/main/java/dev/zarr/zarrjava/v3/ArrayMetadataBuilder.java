package dev.zarr.zarrjava.v3;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;
import dev.zarr.zarrjava.core.codec.core.BytesCodec.Endian;
import dev.zarr.zarrjava.v3.chunkgrid.ChunkGrid;
import dev.zarr.zarrjava.v3.chunkgrid.RegularChunkGrid;
import dev.zarr.zarrjava.v3.chunkkeyencoding.ChunkKeyEncoding;
import dev.zarr.zarrjava.v3.chunkkeyencoding.DefaultChunkKeyEncoding;
import dev.zarr.zarrjava.v3.chunkkeyencoding.V2ChunkKeyEncoding;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ArrayMetadataBuilder {

    long[] shape = null;
    DataType dataType = null;
    ChunkGrid chunkGrid = null;
    ChunkKeyEncoding chunkKeyEncoding =
            new DefaultChunkKeyEncoding(new DefaultChunkKeyEncoding.Configuration(Separator.SLASH));

    Object fillValue = 0;
    Codec[] codecs = new Codec[]{new BytesCodec(Endian.LITTLE)};
    Attributes attributes = new Attributes();
    Map<String, Object>[] storageTransformers = new HashMap[]{};
    String[] dimensionNames = null;

    protected ArrayMetadataBuilder() {
    }

    protected static ArrayMetadataBuilder fromArrayMetadata(ArrayMetadata arrayMetadata) {
        return fromArrayMetadata(arrayMetadata, true);
    }

    protected static ArrayMetadataBuilder fromArrayMetadata(ArrayMetadata arrayMetadata, boolean withAttributes) {
        ArrayMetadataBuilder builder = new ArrayMetadataBuilder();
        builder.shape = arrayMetadata.shape;
        builder.dataType = arrayMetadata.dataType;
        builder.chunkGrid = arrayMetadata.chunkGrid;
        builder.chunkKeyEncoding = arrayMetadata.chunkKeyEncoding;
        builder.fillValue = arrayMetadata.parsedFillValue;
        builder.codecs = arrayMetadata.codecs;
        builder.dimensionNames = arrayMetadata.dimensionNames;
        builder.storageTransformers = arrayMetadata.storageTransformers;
        if (withAttributes) {
            builder.attributes = arrayMetadata.attributes;
        }
        return builder;
    }

    public ArrayMetadataBuilder withShape(long... shape) {
        this.shape = shape;
        return this;
    }

    public ArrayMetadataBuilder withDataType(DataType dataType) {
        this.dataType = dataType;
        return this;
    }

    public ArrayMetadataBuilder withDataType(String dataType) {
        this.dataType = DataType.valueOf(dataType);
        return this;
    }

    public ArrayMetadataBuilder withChunkShape(int... chunkShape) {
        this.chunkGrid = new RegularChunkGrid(new RegularChunkGrid.Configuration(chunkShape));
        return this;
    }

    public ArrayMetadataBuilder withDefaultChunkKeyEncoding(Separator separator) {
        this.chunkKeyEncoding = new DefaultChunkKeyEncoding(
                new DefaultChunkKeyEncoding.Configuration(separator));
        return this;
    }

    public ArrayMetadataBuilder withDefaultChunkKeyEncoding() {
        this.chunkKeyEncoding = new DefaultChunkKeyEncoding(
                new DefaultChunkKeyEncoding.Configuration(Separator.SLASH));
        return this;
    }

    public ArrayMetadataBuilder withDefaultChunkKeyEncoding(String separator) {
        this.chunkKeyEncoding =
                new DefaultChunkKeyEncoding(
                        new DefaultChunkKeyEncoding.Configuration(Separator.valueOf(separator)));
        return this;
    }

    public ArrayMetadataBuilder withV2ChunkKeyEncoding(Separator separator) {
        this.chunkKeyEncoding = new V2ChunkKeyEncoding(new V2ChunkKeyEncoding.Configuration(separator));
        return this;
    }

    public ArrayMetadataBuilder withV2ChunkKeyEncoding() {
        this.chunkKeyEncoding = new V2ChunkKeyEncoding(
                new V2ChunkKeyEncoding.Configuration(Separator.DOT));
        return this;
    }

    public ArrayMetadataBuilder withV2ChunkKeyEncoding(String separator) {
        this.chunkKeyEncoding =
                new V2ChunkKeyEncoding(new V2ChunkKeyEncoding.Configuration(Separator.valueOf(separator)));
        return this;
    }

    public ArrayMetadataBuilder withFillValue(Object fillValue) {
        this.fillValue = fillValue;
        return this;
    }

    public ArrayMetadataBuilder withCodecs(Codec... codecs) {
        this.codecs = codecs;
        return this;
    }

    public ArrayMetadataBuilder withCodecs(Function<CodecBuilder, CodecBuilder> codecBuilder) {
        if (dataType == null) {
            throw new IllegalStateException("Please call `withDataType` first.");
        }
        CodecBuilder nestedCodecBuilder = new CodecBuilder(dataType);
        this.codecs = codecBuilder.apply(nestedCodecBuilder)
                .build();
        return this;
    }

    public ArrayMetadataBuilder withDimensionNames(String... dimensionNames) {
        this.dimensionNames = dimensionNames;
        return this;
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

    public ArrayMetadataBuilder withStorageTransformers(Map<String, Object>[] storageTransformers) {
        this.storageTransformers = storageTransformers;
        return this;
    }

    public ArrayMetadata build() throws ZarrException {
        if (shape == null) {
            throw new ZarrException("Shape needs to be provided. Please call `.withShape`.");
        }
        if (dataType == null) {
            throw new ZarrException("Data type needs to be provided. Please call `.withDataType`.");
        }
        
        // If chunk grid is not specified, calculate default chunks
        if (chunkGrid == null) {
            int[] defaultChunks = calculateDefaultChunks(shape);
            chunkGrid = new RegularChunkGrid(new RegularChunkGrid.Configuration(defaultChunks));
        }
        
        return new ArrayMetadata(shape, dataType, chunkGrid, chunkKeyEncoding, fillValue, codecs,
                dimensionNames,
                attributes,
                storageTransformers
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
