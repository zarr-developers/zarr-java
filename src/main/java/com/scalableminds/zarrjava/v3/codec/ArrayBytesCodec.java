package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.zarrjava.ZarrException;
import com.scalableminds.zarrjava.v3.ArrayMetadata;
import java.nio.ByteBuffer;
import ucar.ma2.Array;

public interface ArrayBytesCodec extends Codec {

  ByteBuffer encode(Array chunkArray, ArrayMetadata.CoreArrayMetadata arrayMetadata)
      throws ZarrException;

  Array decode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata)
      throws ZarrException;

  interface WithPartialEncode extends ArrayBytesCodec {

    ByteBuffer encodePartial(
        Array chunkArray, long[] offset, int[] shape,
        ArrayMetadata.CoreArrayMetadata arrayMetadata
    );
  }

  interface WithPartialDecode extends ArrayBytesCodec {

    Array partialDecode(
        ByteBuffer chunkBytes, long[] offset, int[] shape,
        ArrayMetadata.CoreArrayMetadata arrayMetadata
    );
  }
}

