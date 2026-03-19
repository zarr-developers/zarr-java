package dev.zarr.zarrjava.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class CoordinateTransformation {

    public final String type;
    @Nullable public final String input;
    @Nullable public final String output;
    @Nullable public final String name;
    @Nullable public final List<Double> scale;
    @Nullable public final List<Double> translation;
    @Nullable public final String path;
    @Nullable public final List<CoordinateTransformation> transformations;
    @Nullable public final List<Integer> mapAxis;
    @Nullable public final List<Double> affine;
    @Nullable public final CoordinateTransformation transformation;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CoordinateTransformation(
            @JsonProperty(value = "type", required = true) String type,
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("scale") List<Double> scale,
            @Nullable @JsonProperty("translation") List<Double> translation,
            @Nullable @JsonProperty("path") String path,
            @Nullable @JsonProperty("transformations") List<CoordinateTransformation> transformations,
            @Nullable @JsonProperty("mapAxis") List<Integer> mapAxis,
            @Nullable @JsonProperty("affine") List<Double> affine,
            @Nullable @JsonProperty("transformation") CoordinateTransformation transformation
    ) {
        this.type = type;
        this.input = input;
        this.output = output;
        this.name = name;
        this.scale = scale;
        this.translation = translation;
        this.path = path;
        this.transformations = transformations;
        this.mapAxis = mapAxis;
        this.affine = affine;
        this.transformation = transformation;
    }

    public static CoordinateTransformation scale(List<Double> scale, String input, String output) {
        return new CoordinateTransformation("scale", input, output, null, scale, null, null, null, null, null, null);
    }

    public static CoordinateTransformation translation(List<Double> translation, String input, String output) {
        return new CoordinateTransformation("translation", input, output, null, null, translation, null, null, null, null, null);
    }

    public static CoordinateTransformation identity(String input, String output) {
        return new CoordinateTransformation("identity", input, output, null, null, null, null, null, null, null, null);
    }

    public static CoordinateTransformation sequence(List<CoordinateTransformation> transformations, String input, String output) {
        return new CoordinateTransformation("sequence", input, output, null, null, null, null, transformations, null, null, null);
    }
}
