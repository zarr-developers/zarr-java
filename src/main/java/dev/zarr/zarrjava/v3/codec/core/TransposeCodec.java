package dev.zarr.zarrjava.v3.codec.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.v3.codec.Codec;
import dev.zarr.zarrjava.v3.ArrayMetadata;
import dev.zarr.zarrjava.core.codec.ArrayArrayCodec;
import ucar.ma2.Array;

import javax.annotation.Nonnull;
import java.util.Arrays;

import static dev.zarr.zarrjava.utils.Utils.inversePermutation;
import static dev.zarr.zarrjava.utils.Utils.isPermutation;

public class TransposeCodec extends Codec implements ArrayArrayCodec {

    @Nonnull
    public final String name = "transpose";
    @Nonnull
    public final Configuration configuration;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public TransposeCodec(
            @Nonnull @JsonProperty(value = "configuration", required = true) Configuration configuration
    ) {
        this.configuration = configuration;
    }


    @Override
    public Array decode(Array chunkArray) throws ZarrException {
        if (!isPermutation(configuration.order)){
            throw new ZarrException("Order is no permutation array");
        }
        if (arrayMetadata.ndim() != configuration.order.length) {
            throw new ZarrException("Array has not the same ndim as transpose codec order");
        }
        chunkArray = chunkArray.permute(inversePermutation(configuration.order));
        return chunkArray;
    }



    @Override
    public Array encode(Array chunkArray) throws ZarrException {
        if (!isPermutation(configuration.order)){
            throw new ZarrException("Order is no permutation array");
        }
        if (arrayMetadata.ndim() != configuration.order.length) {
            throw new ZarrException("Array has not the same ndim as transpose codec order");
        }
        chunkArray = chunkArray.permute(configuration.order);
        return chunkArray;
    }

    @Override
    public long computeEncodedSize(long inputByteLength,
                                   ArrayMetadata.CoreArrayMetadata arrayMetadata) throws ZarrException {
        return inputByteLength;
    }

    public static final class Configuration {
        public final int[] order;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Configuration(@JsonProperty(value = "order") int[] order) {
            this.order = order;
        }
    }

    @Override
    public ArrayMetadata.CoreArrayMetadata resolveArrayMetadata() throws ZarrException {
        super.resolveArrayMetadata();
        assert arrayMetadata.ndim() == configuration.order.length;

        int[] transposedChunkShape = new int[arrayMetadata.ndim()];
        Arrays.setAll(transposedChunkShape, i -> arrayMetadata.chunkShape[configuration.order[i]]);

        //only chunk shape gets transformed, the outer shape stays the same
        long[] transposedArrayShape = new long[arrayMetadata.ndim()];
        Arrays.setAll(transposedArrayShape, i -> arrayMetadata.shape[i]/arrayMetadata.chunkShape[i]*transposedArrayShape[i]);

        return new ArrayMetadata.CoreArrayMetadata(
                transposedArrayShape,
                transposedChunkShape,
                arrayMetadata.dataType,
                arrayMetadata.parsedFillValue
        );
    }
}
