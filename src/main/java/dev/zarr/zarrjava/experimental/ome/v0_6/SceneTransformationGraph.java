package dev.zarr.zarrjava.experimental.ome.v0_6;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Lightweight scene transformation graph view for tooling/debugging. */
public final class SceneTransformationGraph {
    public final List<Node> nodes;
    public final List<Edge> edges;
    public final List<String> warnings;

    SceneTransformationGraph(List<Node> nodes, List<Edge> edges, List<String> warnings) {
        this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
        this.edges = Collections.unmodifiableList(new ArrayList<>(edges));
        this.warnings = Collections.unmodifiableList(new ArrayList<>(warnings));
    }

    public static final class Node {
        public final String id;
        public final String groupPath;
        public final String coordinateSystemName;
        public final List<String> axisNames;

        public Node(String id, String groupPath, String coordinateSystemName, List<String> axisNames) {
            this.id = id;
            this.groupPath = groupPath;
            this.coordinateSystemName = coordinateSystemName;
            this.axisNames = axisNames == null
                    ? Collections.<String>emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(axisNames));
        }
    }

    public static final class Edge {
        public final String name;
        public final String type;
        public final String inputNodeId;
        public final String outputNodeId;
        public final String path;

        public Edge(String name, String type, String inputNodeId, String outputNodeId, String path) {
            this.name = name;
            this.type = type;
            this.inputNodeId = inputNodeId;
            this.outputNodeId = outputNodeId;
            this.path = path;
        }
    }
}
