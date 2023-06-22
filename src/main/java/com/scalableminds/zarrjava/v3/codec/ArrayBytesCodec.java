package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.zarrjava.v3.ArrayMetadata;
import ucar.ma2.Array;

import java.nio.ByteBuffer;

public interface ArrayBytesCodec extends Codec {

    ByteBuffer encode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata);

    Array decode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata);

    interface WithPartialEncode extends ArrayBytesCodec {
        ByteBuffer encodePartial(Array chunkArray, long[] offset, int[] shape, ArrayMetadata.CoreArrayMetadata arrayMetadata);
    }

    interface WithPartialDecode extends ArrayBytesCodec {
        Array partialDecode(ByteBuffer chunkBytes, long[] offset, int[] shape, ArrayMetadata.CoreArrayMetadata arrayMetadata);
    }
}

