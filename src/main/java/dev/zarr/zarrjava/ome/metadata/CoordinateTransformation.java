package dev.zarr.zarrjava.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class CoordinateTransformation {

    public final String type;
    @Nullable
    public final List<Double> scale;
    @Nullable
    public final List<Double> translation;
    @Nullable
    public final String path;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CoordinateTransformation(
            @JsonProperty(value = "type", required = true) String type,
            @Nullable @JsonProperty("scale") List<Double> scale,
            @Nullable @JsonProperty("translation") List<Double> translation,
            @Nullable @JsonProperty("path") String path
    ) {
        this.type = type;
        this.scale = scale;
        this.translation = translation;
        this.path = path;
    }

    public static CoordinateTransformation scale(List<Double> scale) {
        return new CoordinateTransformation("scale", scale, null, null);
    }

    public static CoordinateTransformation translation(List<Double> translation) {
        return new CoordinateTransformation("translation", null, translation, null);
    }

    public static CoordinateTransformation identity() {
        return new CoordinateTransformation("identity", null, null, null);
    }
}
