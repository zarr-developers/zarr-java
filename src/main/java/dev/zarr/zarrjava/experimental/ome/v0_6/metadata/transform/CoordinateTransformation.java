package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;

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
public interface CoordinateTransformation {
    String getType();
    String getInput();
    String getOutput();
    String getName();

    static CoordinateTransformation scale(List<Double> scale, String input, String output) {
        return new ScaleCoordinateTransformation(input, output, null, scale, null);
    }

    static CoordinateTransformation translation(List<Double> translation, String input, String output) {
        return new TranslationCoordinateTransformation(input, output, null, translation, null);
    }

    static CoordinateTransformation identity(String input, String output) {
        return new IdentityCoordinateTransformation(input, output, null, null);
    }
}
