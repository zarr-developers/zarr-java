package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Projects input coordinates from N to M dimensions by dropping input dimensions and/or
 * creating new output dimensions.
 */
public final class ProjectAxisCoordinateTransformation extends BaseCoordinateTransformation {
    /** Indices of the input coordinate vector at which dimensions are dropped. */
    @Nullable public final List<Integer> droppedInputs;
    /** Indices of the output coordinate vector at which new dimensions are added. */
    @Nullable public final List<Integer> createdOutputs;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ProjectAxisCoordinateTransformation(
            @Nullable @JsonProperty("input") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String input,
            @Nullable @JsonProperty("output") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("droppedInputs") List<Integer> droppedInputs,
            @Nullable @JsonProperty("createdOutputs") List<Integer> createdOutputs
    ) {
        super("projectAxis", input, output, name);
        this.droppedInputs = droppedInputs;
        this.createdOutputs = createdOutputs;
    }
}
