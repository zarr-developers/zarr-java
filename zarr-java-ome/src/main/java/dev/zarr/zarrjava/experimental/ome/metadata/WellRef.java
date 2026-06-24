package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A reference to a well within a plate, identified by path and row/column indices. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class WellRef {

    public final String path;
    public final int rowIndex;
    public final int columnIndex;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public WellRef(
            @JsonProperty(value = "path", required = true) String path,
            @JsonProperty(value = "rowIndex", required = true) int rowIndex,
            @JsonProperty(value = "columnIndex", required = true) int columnIndex
    ) {
        this.path = path;
        this.rowIndex = rowIndex;
        this.columnIndex = columnIndex;
    }
}
