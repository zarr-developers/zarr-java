package dev.zarr.zarrjava.v2;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Endianness {
    LITTLE("<"), BIG(">"), UNSPECIFIED("|");

    private final String value;

    Endianness(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}