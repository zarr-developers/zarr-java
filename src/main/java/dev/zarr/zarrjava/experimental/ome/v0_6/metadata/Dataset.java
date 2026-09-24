package dev.zarr.zarrjava.experimental.ome.v0_6.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateTransformation;

import java.io.IOException;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class Dataset {

    public final String path;
    @JsonProperty("coordinateTransformations")
    public final List<CoordinateTransformation> coordinateTransformations;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Dataset(
            @JsonProperty(value = "path", required = true) String path,
            @JsonProperty(value = "coordinateTransformations", required = true)
            @JsonDeserialize(using = TransformationsDeserializer.class)
            List<CoordinateTransformation> coordinateTransformations
    ) {
        this.path = path;
        this.coordinateTransformations = coordinateTransformations;
    }

    /**
     * Reads dataset-level transformations, mapping the legacy (pre-release 0.6) bare-string
     * {@code "input": "<dataset path>"} to {@code {"path": "<dataset path>"}}. Everywhere else a bare
     * string reference is read as a coordinate system name (see
     * {@link dev.zarr.zarrjava.experimental.ome.v0_6.metadata.transform.CoordinateSystemRef}).
     */
    static final class TransformationsDeserializer extends JsonDeserializer<List<CoordinateTransformation>> {
        @Override
        public List<CoordinateTransformation> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode tree = p.readValueAsTree();
            if (tree != null && tree.isArray()) {
                for (JsonNode item : tree) {
                    if (item instanceof ObjectNode) {
                        ObjectNode obj = (ObjectNode) item;
                        JsonNode input = obj.get("input");
                        if (input != null && input.isTextual()) {
                            ObjectNode ref = obj.objectNode();
                            ref.put("path", input.asText());
                            obj.set("input", ref);
                        }
                    }
                }
            }
            JavaType type = ctxt.getTypeFactory().constructCollectionType(List.class, CoordinateTransformation.class);
            return ctxt.readTreeAsValue(tree, type);
        }
    }
}
