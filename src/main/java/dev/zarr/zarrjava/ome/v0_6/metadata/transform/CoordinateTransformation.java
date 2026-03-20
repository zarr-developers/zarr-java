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

}
