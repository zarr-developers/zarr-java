package com.scalableminds.zarrjava.v3.codec;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
public interface Codec {

}

