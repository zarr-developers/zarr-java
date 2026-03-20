package dev.zarr.zarrjava.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import dev.zarr.zarrjava.v3.Node;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class SceneCoordinateTransformation {

    public final String type;
    @Nullable public final CoordinateSystemRef input;
    @Nullable public final CoordinateSystemRef output;
    @Nullable public final String name;

    @Nullable public final List<Double> scale;
    @Nullable public final List<Double> translation;
    @Nullable public final List<List<Double>> affine;
    @Nullable public final List<List<Double>> rotation;
    @Nullable public final String path;

    @Nullable @JsonIgnore public final List<SceneCoordinateTransformation> sequenceTransformations;
    @Nullable public final List<Integer> mapAxis;
    @Nullable public final SceneCoordinateTransformation transformation;
    @Nullable public final SceneCoordinateTransformation forward;
    @Nullable public final SceneCoordinateTransformation inverse;
    @Nullable @JsonIgnore public final List<ByDimensionTransformation> byDimensionTransformations;

    public final Map<String, Object> raw = new LinkedHashMap<>();

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SceneCoordinateTransformation(
            @JsonProperty(value = "type", required = true) String type,
            @Nullable @JsonProperty("input") CoordinateSystemRef input,
            @Nullable @JsonProperty("output") CoordinateSystemRef output,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("scale") List<Double> scale,
            @Nullable @JsonProperty("translation") List<Double> translation,
            @Nullable @JsonProperty("affine") List<List<Double>> affine,
            @Nullable @JsonProperty("rotation") List<List<Double>> rotation,
            @Nullable @JsonProperty("path") String path,
            @Nullable @JsonProperty("transformations") JsonNode transformationsNode,
            @Nullable @JsonProperty("mapAxis") List<Integer> mapAxis,
            @Nullable @JsonProperty("transformation") SceneCoordinateTransformation transformation,
            @Nullable @JsonProperty("forward") SceneCoordinateTransformation forward,
            @Nullable @JsonProperty("inverse") SceneCoordinateTransformation inverse
    ) {
        this.type = type;
        this.input = input;
        this.output = output;
        this.name = name;
        this.scale = scale;
        this.translation = translation;
        this.affine = affine;
        this.rotation = rotation;
        this.path = path;
        this.sequenceTransformations = parseSequenceTransformations(transformationsNode);
        this.byDimensionTransformations = parseByDimensionTransformations(transformationsNode);
        this.mapAxis = mapAxis;
        this.transformation = transformation;
        this.forward = forward;
        this.inverse = inverse;
    }

    @JsonAnySetter
    public void capture(String key, Object value) {
        raw.put(key, value);
    }

    @JsonProperty("transformations")
    @Nullable
    public Object getTransformationsForJson() {
        if (sequenceTransformations != null) {
            return sequenceTransformations;
        }
        return byDimensionTransformations;
    }

    @Nullable
    private static List<SceneCoordinateTransformation> parseSequenceTransformations(@Nullable JsonNode node) {
        if (node == null || !node.isArray() || node.size() == 0 || !node.get(0).has("type")) {
            return null;
        }
        return Node.makeObjectMapper().convertValue(
                node, Node.makeObjectMapper().getTypeFactory()
                        .constructCollectionType(List.class, SceneCoordinateTransformation.class));
    }

    @Nullable
    private static List<ByDimensionTransformation> parseByDimensionTransformations(@Nullable JsonNode node) {
        if (node == null || !node.isArray() || node.size() == 0 || node.get(0).has("type")) {
            return null;
        }
        return Node.makeObjectMapper().convertValue(
                node, Node.makeObjectMapper().getTypeFactory()
                        .constructCollectionType(List.class, ByDimensionTransformation.class));
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static final class ByDimensionTransformation {
        @Nullable @JsonProperty("input_axes") public final List<Integer> inputAxes;
        @Nullable @JsonProperty("output_axes") public final List<Integer> outputAxes;
        @Nullable public final SceneCoordinateTransformation transformation;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public ByDimensionTransformation(
                @Nullable @JsonProperty("input_axes") List<Integer> inputAxes,
                @Nullable @JsonProperty("output_axes") List<Integer> outputAxes,
                @Nullable @JsonProperty("transformation") SceneCoordinateTransformation transformation
        ) {
            this.inputAxes = inputAxes;
            this.outputAxes = outputAxes;
            this.transformation = transformation;
        }
    }
}
