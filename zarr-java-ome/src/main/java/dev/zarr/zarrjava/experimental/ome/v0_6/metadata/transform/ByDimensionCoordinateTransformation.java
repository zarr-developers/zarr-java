package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import javax.annotation.Nullable;
import java.util.List;

public final class ByDimensionCoordinateTransformation extends BaseCoordinateTransformation {
    @Nullable public final List<ByDimensionTransformation> transformations;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ByDimensionCoordinateTransformation(
            @Nullable @JsonProperty("input") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String input,
            @Nullable @JsonProperty("output") @JsonDeserialize(using = CoordinateSystemRefSerde.Deserializer.class) String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("transformations") List<ByDimensionTransformation> transformations
    ) {
        super("byDimension", input, output, name);
        this.transformations = transformations;
    }

    public static final class ByDimensionTransformation {
        @Nullable public final List<Integer> inputAxes;
        @Nullable public final List<Integer> outputAxes;
        @Nullable public final CoordinateTransformation transformation;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public ByDimensionTransformation(
                @Nullable @JsonProperty("input_axes") List<Integer> inputAxes,
                @Nullable @JsonProperty("output_axes") List<Integer> outputAxes,
                @Nullable @JsonProperty("transformation") CoordinateTransformation transformation
        ) {
            this.inputAxes = inputAxes;
            this.outputAxes = outputAxes;
            this.transformation = transformation;
        }
    }
}
