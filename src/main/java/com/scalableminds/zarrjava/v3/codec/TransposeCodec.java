package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.zarrjava.v3.ArrayMetadata;
import com.scalableminds.zarrjava.v3.codec.Codec.ArrayArrayCodec;
import ucar.ma2.Array;

import java.util.stream.IntStream;

public class TransposeCodec extends ArrayArrayCodec {
    public final String name = "transpose";
    public Configuration configuration;

    @Override
    public Array innerDecode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        if (configuration.order.equals("F")) {
            int[] dims = IntStream.range(arrayMetadata.ndim() - 1, -1).toArray();
            chunkArray.permute(dims);
        }
        return chunkArray;
    }

    @Override
    public Array innerEncode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata) {
        if (configuration.order.equals("F")) {
            int[] dims = IntStream.range(arrayMetadata.ndim() - 1, -1).toArray();
            chunkArray.permute(dims);
        }
        return chunkArray;
    }

    public static final class Configuration {
        public String order = "C";
    }
}
