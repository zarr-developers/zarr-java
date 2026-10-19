package dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;
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
    @Nullable CoordinateSystemRef getInput();
    @Nullable CoordinateSystemRef getOutput();
    String getName();

    /**
     * Scale transformation from {@code input} to {@code output}. For a {@code multiscales > datasets}
     * entry use {@code CoordinateSystemRef.ofPath(datasetPath)} as input and
     * {@code CoordinateSystemRef.ofName(intrinsicName)} as output.
     */
    static CoordinateTransformation scale(
            List<Double> scale, @Nullable CoordinateSystemRef input, @Nullable CoordinateSystemRef output) {
        return new ScaleCoordinateTransformation(input, output, null, scale, null);
    }

    /** Scale transformation without input/output, e.g. for use inside a {@code sequence}. */
    static CoordinateTransformation scale(List<Double> scale) {
        return scale(scale, null, null);
    }

    static CoordinateTransformation translation(
            List<Double> translation, @Nullable CoordinateSystemRef input, @Nullable CoordinateSystemRef output) {
        return new TranslationCoordinateTransformation(input, output, null, translation, null);
    }

    /** Translation transformation without input/output, e.g. for use inside a {@code sequence}. */
    static CoordinateTransformation translation(List<Double> translation) {
        return translation(translation, null, null);
    }

    static CoordinateTransformation identity(@Nullable CoordinateSystemRef input, @Nullable CoordinateSystemRef output) {
        return new IdentityCoordinateTransformation(input, output, null, null);
    }

    static CoordinateTransformation sequence(
            List<CoordinateTransformation> transformations,
            @Nullable CoordinateSystemRef input,
            @Nullable CoordinateSystemRef output) {
        return new SequenceCoordinateTransformation(input, output, null, transformations);
    }
}
