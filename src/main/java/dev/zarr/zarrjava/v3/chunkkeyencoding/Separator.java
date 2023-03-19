package dev.zarr.zarrjava.v3.chunkkeyencoding;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Separator {
    SLASH("/"), DOT(".");

    private String value;

    private Separator(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }
}
