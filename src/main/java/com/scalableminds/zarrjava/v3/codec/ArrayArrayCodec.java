package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.zarrjava.v3.ArrayMetadata;
import ucar.ma2.Array;

public interface ArrayArrayCodec extends Codec {

    Array encode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata);

    Array decode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata);

}
