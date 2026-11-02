package dev.zarr.zarrjava.v3;

import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;
import dev.zarr.zarrjava.core.codec.core.BytesCodec.Endian;
import dev.zarr.zarrjava.utils.Utils;
import dev.zarr.zarrjava.v3.chunkgrid.ChunkGrid;
import dev.zarr.zarrjava.v3.chunkgrid.RegularChunkGrid;
import dev.zarr.zarrjava.v3.chunkkeyencoding.ChunkKeyEncoding;
import dev.zarr.zarrjava.v3.chunkkeyencoding.DefaultChunkKeyEncoding;
import dev.zarr.zarrjava.v3.chunkkeyencoding.V2ChunkKeyEncoding;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.codec.CodecBuilder;
import dev.zarr.zarrjava.v3.codec.core.BytesCodec;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ArrayMetadataBuilder {

    long[] shape = null;
    long[] lowerBound = null;
    DataType dataType = null;
    ChunkGrid chunkGrid = null;
    ChunkKeyEncoding chunkKeyEncoding =
            new DefaultChunkKeyEncoding(new DefaultChunkKeyEncoding.Configuration(Separator.SLASH));

    Object fillValue = 0;
    Codec[] codecs = new Codec[]{new BytesCodec(Endian.LITTLE)};
    Attributes attributes = new Attributes();
    Map<String, Object>[] storageTransformers = new HashMap[]{};
    String[] dimensionNames = null;
    Map<String, Object> extraFields = null;

    protected ArrayMetadataBuilder() {
    }

    protected static ArrayMetadataBuilder fromArrayMetadata(ArrayMetadata arrayMetadata) {
        return fromArrayMetadata(arrayMetadata, true);
    }

    protected static ArrayMetadataBuilder fromArrayMetadata(ArrayMetadata arrayMetadata, boolean withAttributes) {
        ArrayMetadataBuilder builder = new ArrayMetadataBuilder();
        builder.shape = arrayMetadata.shape;
        builder.lowerBound = arrayMetadata.lowerBound();
        builder.dataType = arrayMetadata.dataType;
        builder.chunkGrid = arrayMetadata.chunkGrid;
        builder.chunkKeyEncoding = arrayMetadata.chunkKeyEncoding;
        builder.fillValue = arrayMetadata.parsedFillValue;
        builder.codecs = arrayMetadata.codecs;
        builder.dimensionNames = arrayMetadata.dimensionNames;
        builder.storageTransformers = arrayMetadata.storageTransformers;
        builder.extraFields = arrayMetadata.extraFields();
        if (withAttributes) {
            builder.attributes = arrayMetadata.attributes;
        }
        return builder;
    }

    /**
     * Sets the shape of the array. A lower bound set earlier, e.g. through {@link #withBounds}, is
     * kept.
     */
    public ArrayMetadataBuilder withShape(long... shape) {
        this.shape = shape;
        return this;
    }

    /**
     * Sets the coordinate range of the array: the first and one past the last coordinate along each
     * dimension. The lower bound may be negative.
     */
    public ArrayMetadataBuilder withBounds(long[] inclusiveMin, long[] exclusiveMax) throws ZarrException {
        if (inclusiveMin.length != exclusiveMax.length) {
            throw new ZarrException("'inclusiveMin' and 'exclusiveMax' need to have the same length.");
        }
        long[] shape = new long[inclusiveMin.length];
        for (int i = 0; i < shape.length; i++) {
            shape[i] = exclusiveMax[i] - inclusiveMin[i];
        }
        this.shape = shape;
        this.lowerBound = inclusiveMin;
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

    /**
     * Sets members of the metadata document that zarr-java itself does not interpret. Every value
     * needs to be a map carrying {@code "must_understand": false}, otherwise {@link #build()} fails.
     *
     * @param extraFields the extra fields to write into the metadata document
     */
    public ArrayMetadataBuilder withExtraFields(Map<String, Object> extraFields) {
        this.extraFields = extraFields;
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
            int[] defaultChunks = Utils.calculateDefaultChunks(shape);
            chunkGrid = new RegularChunkGrid(new RegularChunkGrid.Configuration(defaultChunks));
        }
        
        ArrayMetadata.Bounds bounds = null;
        if (lowerBound != null && Arrays.stream(lowerBound).anyMatch(b -> b != 0)) {
            if (lowerBound.length != shape.length) {
                throw new ZarrException("The lower bound and the shape need to have the same rank.");
            }
            long[] exclusiveMax = new long[shape.length];
            for (int i = 0; i < shape.length; i++) {
                exclusiveMax[i] = lowerBound[i] + shape[i];
            }
            bounds = new ArrayMetadata.Bounds(lowerBound, exclusiveMax);
        }

        return new ArrayMetadata(ArrayMetadata.ZARR_FORMAT, ArrayMetadata.NODE_TYPE,
                bounds == null ? shape : null, bounds, dataType, chunkGrid, chunkKeyEncoding, fillValue,
                codecs, dimensionNames, attributes, storageTransformers, extraFields
        );
    }
}
