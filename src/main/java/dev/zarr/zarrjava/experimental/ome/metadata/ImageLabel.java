package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * OME-Zarr {@code image-label} metadata of a label image (display colors, per-label properties
 * and the source image).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class ImageLabel {

    @Nullable
    public final List<Color> colors;
    @Nullable
    public final List<Property> properties;
    @Nullable
    public final Source source;
    /** Older OME-Zarr versions carried a {@code version} inside {@code image-label}; kept for round-tripping. */
    @Nullable
    public final String version;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ImageLabel(
            @Nullable @JsonProperty("colors") List<Color> colors,
            @Nullable @JsonProperty("properties") List<Property> properties,
            @Nullable @JsonProperty("source") Source source,
            @Nullable @JsonProperty("version") String version
    ) {
        this.colors = colors;
        this.properties = properties;
        this.source = source;
        this.version = version;
    }

    public ImageLabel(
            @Nullable List<Color> colors,
            @Nullable List<Property> properties,
            @Nullable Source source
    ) {
        this(colors, properties, source, null);
    }

    /** Display color of a single label value. Additional keys are preserved. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Color {
        @JsonProperty("label-value")
        public final int labelValue;
        @Nullable
        public final List<Integer> rgba;
        private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Color(
                @JsonProperty(value = "label-value", required = true) int labelValue,
                @Nullable @JsonProperty("rgba") List<Integer> rgba
        ) {
            this.labelValue = labelValue;
            this.rgba = rgba;
        }

        /** Keys other than {@code label-value} and {@code rgba}. */
        @JsonAnyGetter
        public Map<String, Object> getAdditionalProperties() {
            return additionalProperties;
        }

        @JsonAnySetter
        public void setAdditionalProperty(String key, Object value) {
            additionalProperties.put(key, value);
        }
    }

    /** Arbitrary properties associated with a single label value. */
    public static final class Property {
        @JsonProperty("label-value")
        public final int labelValue;
        private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Property(@JsonProperty(value = "label-value", required = true) int labelValue) {
            this.labelValue = labelValue;
        }

        /** Keys other than {@code label-value}. */
        @JsonAnyGetter
        public Map<String, Object> getAdditionalProperties() {
            return additionalProperties;
        }

        @JsonAnySetter
        public void setAdditionalProperty(String key, Object value) {
            additionalProperties.put(key, value);
        }
    }

    /** Reference to the image the label image was derived from. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class Source {
        /** Spec default for {@link #image} when absent. */
        public static final String DEFAULT_IMAGE = "../../";

        /** Relative path to the source image group, or null if absent (spec default {@code ../../}). */
        @Nullable
        public final String image;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public Source(@Nullable @JsonProperty("image") String image) {
            this.image = image;
        }

        /** Returns {@link #image}, falling back to the spec default {@code ../../}. */
        public String resolveImage() {
            return image != null ? image : DEFAULT_IMAGE;
        }
    }
}
