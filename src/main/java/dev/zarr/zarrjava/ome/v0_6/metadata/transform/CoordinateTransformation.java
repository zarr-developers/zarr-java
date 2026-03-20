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
        @JsonSubTypes.Type(value = AffineCoordinateTransformation.class, name = "affine")
})
public abstract class CoordinateTransformation {

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

    protected CoordinateTransformation(
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
        return new ScaleCoordinateTransformation(input, output, null, scale, null);
    }

    public static CoordinateTransformation translation(List<Double> translation, String input, String output) {
        return new TranslationCoordinateTransformation(input, output, null, translation, null);
    }

    public static CoordinateTransformation identity(String input, String output) {
        return new IdentityCoordinateTransformation(input, output, null);
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
            @Nullable List<Double> affine,
            @Nullable CoordinateTransformation transformation
    ) {
        if ("scale".equals(type)) {
            return new ScaleCoordinateTransformation(input, output, name, scale, path);
        }
        if ("translation".equals(type)) {
            return new TranslationCoordinateTransformation(input, output, name, translation, path);
        }
        if ("identity".equals(type)) {
            return new IdentityCoordinateTransformation(input, output, path);
        }
        if ("sequence".equals(type)) {
            return new SequenceCoordinateTransformation(input, output, name, transformations);
        }
        if ("mapAxis".equals(type)) {
            return new MapAxisCoordinateTransformation(input, output, name, mapAxis, transformation);
        }
        if ("affine".equals(type)) {
            return new AffineCoordinateTransformation(input, output, name, affine);
        }
        return new GenericCoordinateTransformation(
                type, input, output, name, scale, translation, path, transformations, mapAxis, affine, transformation);
    }
}
