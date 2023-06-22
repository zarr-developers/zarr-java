package com.scalableminds.zarrjava.v3.codec;

import com.scalableminds.zarrjava.v3.ArrayMetadata;

import java.nio.ByteBuffer;

public interface BytesBytesCodec extends Codec {
    ByteBuffer encode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata);

    ByteBuffer decode(ByteBuffer chunkBytes, ArrayMetadata.CoreArrayMetadata arrayMetadata);

}
