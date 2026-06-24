package dev.zarr.zarrjava.experimental.ome.metadata.transform;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

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
        @JsonSubTypes.Type(value = IdentityCoordinateTransformation.class, name = "identity")
})
public abstract class CoordinateTransformation {

    public final String type;

    protected CoordinateTransformation(
            @JsonProperty(value = "type", required = true) String type
    ) {
        this.type = type;
    }

    public static CoordinateTransformation scale(List<Double> scale) {
        return new ScaleCoordinateTransformation(scale, null);
    }

    public static CoordinateTransformation translation(List<Double> translation) {
        return new TranslationCoordinateTransformation(translation, null);
    }

    public static CoordinateTransformation identity() {
        return new IdentityCoordinateTransformation(null);
    }

}
