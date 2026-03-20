package dev.zarr.zarrjava.ome.metadata.transform;

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

    public static CoordinateTransformation fromRaw(
            String type,
            @Nullable List<Double> scale,
            @Nullable List<Double> translation,
            @Nullable String path
    ) {
        if ("scale".equals(type)) {
            return new ScaleCoordinateTransformation(scale, path);
        }
        if ("translation".equals(type)) {
            return new TranslationCoordinateTransformation(translation, path);
        }
        if ("identity".equals(type)) {
            return new IdentityCoordinateTransformation(path);
        }
        GenericCoordinateTransformation generic = new GenericCoordinateTransformation(type);
        if (scale != null) {
            generic.raw.put("scale", scale);
        }
        if (translation != null) {
            generic.raw.put("translation", translation);
        }
        if (path != null) {
            generic.raw.put("path", path);
        }
        return generic;
    }
}
