package dev.zarr.zarrjava.core.chunkkeyencoding;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Separator {
    SLASH("/"),
    DOT(".");

    private final String value;

    Separator(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
