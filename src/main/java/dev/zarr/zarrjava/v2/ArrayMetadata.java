package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.core.chunkkeyencoding.ChunkKeyEncoding;
import dev.zarr.zarrjava.core.chunkkeyencoding.Separator;
import dev.zarr.zarrjava.utils.MultiArrayUtils;
import dev.zarr.zarrjava.v2.chunkkeyencoding.V2ChunkKeyEncoding;
import dev.zarr.zarrjava.v2.codec.Codec;
import ucar.ma2.Array;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class ArrayMetadata extends dev.zarr.zarrjava.core.ArrayMetadata {
    static final int ZARR_FORMAT = 2;

    @JsonProperty("zarr_format")
    public final int zarrFormat = ZARR_FORMAT;

    public final int[] chunks;

    @JsonProperty("dtype")
    public final DataType dataType;

    @JsonIgnore
    public final Endianness endianness;

    @JsonProperty("order")
    public final Order order;

    @JsonProperty("dimension_separator")
    public final Separator dimensionSeparator;

    @Nullable
    public final Codec[] filters;
    @Nullable
    public final Codec compressor;

    @Nullable
    @JsonIgnore
    public Attributes attributes;

    @JsonIgnore
    public CoreArrayMetadata coreArrayMetadata;


    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ArrayMetadata(
            @JsonProperty(value = "zarr_format", required = true) int zarrFormat,
            @JsonProperty(value = "shape", required = true) long[] shape,
            @JsonProperty(value = "chunks", required = true) int[] chunks,
            @JsonProperty(value = "dtype", required = true) DataType dataType,
            @Nullable @JsonProperty(value = "fill_value", required = true) Object fillValue,
            @JsonProperty(value = "order", required = true) Order order,
            @Nullable @JsonProperty(value = "filters", required = true) Codec[] filters,
            @Nullable @JsonProperty(value = "compressor", required = true) Codec compressor,
            @Nullable @JsonProperty(value = "dimension_separator") Separator dimensionSeparator
    ) throws ZarrException {
        this(zarrFormat, shape, chunks, dataType, fillValue, order, filters, compressor, dimensionSeparator, null);
    }


    public ArrayMetadata(
            int zarrFormat,
            long[] shape,
            int[] chunks,
            DataType dataType,
            @Nullable Object fillValue,
            Order order,
            @Nullable Codec[] filters,
            @Nullable Codec compressor,
            @Nullable Separator dimensionSeparator,
            @Nullable Attributes attributes
    ) throws ZarrException {
        super(shape, fillValue, dataType);
        if (zarrFormat != this.zarrFormat) {
            throw new ZarrException(
                    "Expected zarr format '" + this.zarrFormat + "', got '" + zarrFormat + "'.");
        }
        this.chunks = chunks;
        this.dataType = dataType;
        this.endianness = dataType.getEndianness();
        this.order = order;
        this.dimensionSeparator = dimensionSeparator;
        this.coreArrayMetadata =
                new CoreArrayMetadata(shape, chunks,
                        this.dataType,
                        this.parsedFillValue
                );
        if (filters == null) this.filters = null;
        else {
            this.filters = new Codec[filters.length];
            for (int i = 0; i < filters.length; i++) {
                this.filters[i] = filters[i].evolveFromCoreArrayMetadata(this.coreArrayMetadata);
            }
        }
        this.compressor = compressor == null ? null : compressor.evolveFromCoreArrayMetadata(this.coreArrayMetadata);
        this.attributes = attributes;
    }


    @Override
    public int[] chunkShape() {
        return chunks;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public Array allocateFillValueChunk() {
        Array outputArray = Array.factory(dataType.getMA2DataType(), chunks);
        if (parsedFillValue != null) MultiArrayUtils.fill(outputArray, parsedFillValue);
        return outputArray;
    }

    @Override
    public ChunkKeyEncoding chunkKeyEncoding() {
        Separator separator = dimensionSeparator == null ? Separator.DOT : dimensionSeparator;
        return new V2ChunkKeyEncoding(separator);
    }

    @Override
    public Object parsedFillValue() {
        return parsedFillValue;
    }

    @Override
    public @Nonnull Attributes attributes() throws ZarrException {
        if (attributes == null) {
            throw new ZarrException("Array attributes have not been set.");
        }
        return attributes;
    }


}
