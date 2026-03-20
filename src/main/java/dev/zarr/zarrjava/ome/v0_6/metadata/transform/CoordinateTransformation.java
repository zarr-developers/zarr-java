package dev.zarr.zarrjava.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type",
        visible = true,
        defaultImpl = GenericCoordinateTransformation.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ScaleCoordinateTransformation.class, name = "scale"),
        @JsonSubTypes.Type(value = TranslationCoordinateTransformation.class, name = "translation"),
        @JsonSubTypes.Type(value = IdentityCoordinateTransformation.class, name = "identity"),
        @JsonSubTypes.Type(value = SequenceCoordinateTransformation.class, name = "sequence"),
        @JsonSubTypes.Type(value = MapAxisCoordinateTransformation.class, name = "mapAxis"),
        @JsonSubTypes.Type(value = AffineCoordinateTransformation.class, name = "affine"),
        @JsonSubTypes.Type(value = RotationCoordinateTransformation.class, name = "rotation"),
        @JsonSubTypes.Type(value = DisplacementsCoordinateTransformation.class, name = "displacements"),
        @JsonSubTypes.Type(value = CoordinatesCoordinateTransformation.class, name = "coordinates"),
        @JsonSubTypes.Type(value = BijectionCoordinateTransformation.class, name = "bijection"),
        @JsonSubTypes.Type(value = ByDimensionCoordinateTransformation.class, name = "byDimension")
})
public abstract class CoordinateTransformation {

    public final String type;
    @Nullable public final String input;
    @Nullable public final String output;
    @Nullable public final String name;

    protected CoordinateTransformation(
            @JsonProperty(value = "type", required = true) String type,
            @Nullable @JsonProperty("input") String input,
            @Nullable @JsonProperty("output") String output,
            @Nullable @JsonProperty("name") String name
    ) {
        this.type = type;
        this.input = input;
        this.output = output;
        this.name = name;
    }

    public static CoordinateTransformation scale(List<Double> scale, String input, String output) {
        return new ScaleCoordinateTransformation(input, output, null, scale, null);
    }

    public static CoordinateTransformation translation(List<Double> translation, String input, String output) {
        return new TranslationCoordinateTransformation(input, output, null, translation, null);
    }

    public static CoordinateTransformation identity(String input, String output) {
        return new IdentityCoordinateTransformation(input, output, null, null);
    }

    public static CoordinateTransformation sequence(List<CoordinateTransformation> transformations, String input, String output) {
        return new SequenceCoordinateTransformation(input, output, null, transformations);
    }

    public static CoordinateTransformation fromRaw(
            String type,
            @Nullable String input,
            @Nullable String output,
            @Nullable String name,
            @Nullable List<Double> scale,
            @Nullable List<Double> translation,
            @Nullable String path,
            @Nullable List<CoordinateTransformation> transformations,
            @Nullable List<Integer> mapAxis,
            @Nullable List<List<Double>> affine,
            @Nullable List<List<Double>> matrix,
            @Nullable CoordinateTransformation forward,
            @Nullable CoordinateTransformation inverse,
            @Nullable List<Integer> inputAxes,
            @Nullable List<Integer> outputAxes,
            @Nullable CoordinateTransformation transformation
    ) {
        if ("scale".equals(type)) {
            return new ScaleCoordinateTransformation(input, output, name, scale, path);
        }
        if ("translation".equals(type)) {
            return new TranslationCoordinateTransformation(input, output, name, translation, path);
        }
        if ("identity".equals(type)) {
            return new IdentityCoordinateTransformation(input, output, name, path);
        }
        if ("sequence".equals(type)) {
            return new SequenceCoordinateTransformation(input, output, name, transformations);
        }
        if ("mapAxis".equals(type)) {
            return new MapAxisCoordinateTransformation(input, output, name, mapAxis, transformation);
        }
        if ("affine".equals(type)) {
            return new AffineCoordinateTransformation(input, output, name, affine, path);
        }
        if ("rotation".equals(type)) {
            return new RotationCoordinateTransformation(input, output, name, matrix, path);
        }
        if ("displacements".equals(type)) {
            return new DisplacementsCoordinateTransformation(input, output, name, path);
        }
        if ("coordinates".equals(type)) {
            return new CoordinatesCoordinateTransformation(input, output, name, path);
        }
        if ("bijection".equals(type)) {
            return new BijectionCoordinateTransformation(input, output, name, forward, inverse);
        }
        if ("byDimension".equals(type)) {
            List<ByDimensionCoordinateTransformation.ByDimensionTransformation> byDimTransformations = null;
            if (transformation != null) {
                byDimTransformations = java.util.Collections.singletonList(
                        new ByDimensionCoordinateTransformation.ByDimensionTransformation(inputAxes, outputAxes, transformation));
            }
            return new ByDimensionCoordinateTransformation(input, output, name, byDimTransformations);
        }
        GenericCoordinateTransformation generic = new GenericCoordinateTransformation(type, input, output, name);
        if (scale != null) generic.raw.put("scale", scale);
        if (translation != null) generic.raw.put("translation", translation);
        if (path != null) generic.raw.put("path", path);
        if (transformations != null) generic.raw.put("transformations", transformations);
        if (mapAxis != null) generic.raw.put("mapAxis", mapAxis);
        if (affine != null) generic.raw.put("affine", affine);
        if (matrix != null) generic.raw.put("rotation", matrix);
        if (forward != null) generic.raw.put("forward", forward);
        if (inverse != null) generic.raw.put("inverse", inverse);
        if (inputAxes != null) generic.raw.put("input_axes", inputAxes);
        if (outputAxes != null) generic.raw.put("output_axes", outputAxes);
        if (transformation != null) generic.raw.put("transformation", transformation);
        return generic;
    }

    public static CoordinateTransformation fromRaw(
            String type,
            @Nullable String input,
            @Nullable String output,
            @Nullable String name,
            @Nullable List<Double> scale,
            @Nullable List<Double> translation,
            @Nullable String path,
            @Nullable List<CoordinateTransformation> transformations,
            @Nullable List<Integer> mapAxis,
            @Nullable List<List<Double>> affine,
            @Nullable CoordinateTransformation transformation
    ) {
        return fromRaw(type, input, output, name, scale, translation, path, transformations, mapAxis, affine,
                null, null, null, null, null, transformation);
    }
}
