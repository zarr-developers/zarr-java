package dev.zarr.zarrjava.experimental.ome.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/** Omero display metadata stored in OME-Zarr attributes. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class OmeroMetadata {
    private static final Logger LOGGER = Logger.getLogger(OmeroMetadata.class.getName());
    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();

    @Nullable
    public final Integer id;
    @Nullable
    public final String version;
    @Nullable
    public final String name;
    @Nullable
    public final List<OmeroChannel> channels;
    @Nullable
    public final OmeroRdefs rdefs;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public OmeroMetadata(
            @Nullable @JsonProperty("id") Integer id,
            @Nullable @JsonProperty("version") String version,
            @Nullable @JsonProperty("name") String name,
            @Nullable @JsonProperty("channels") List<OmeroChannel> channels,
            @Nullable @JsonProperty("rdefs") OmeroRdefs rdefs
    ) {
        this.id = id;
        this.version = version;
        this.name = name;
        this.channels = channels;
        this.rdefs = rdefs;
        warnIfRequiredFieldsMissing();
    }

    public OmeroMetadata(
            @Nullable List<OmeroChannel> channels,
            @Nullable OmeroRdefs rdefs
    ) {
        this(null, null, null, channels, rdefs);
    }

    private void warnIfRequiredFieldsMissing() {
        if (channels == null) {
            warnOnce("missing-channels", "OMERO metadata is present but missing required field 'channels'.");
            return;
        }
        for (int i = 0; i < channels.size(); i++) {
            OmeroChannel channel = channels.get(i);
            if (channel == null) {
                warnOnce("channel-null", "OMERO metadata channel[" + i + "] is null; required fields 'color' and 'window' are missing.");
                continue;
            }
            if (channel.color == null || channel.color.isEmpty()) {
                warnOnce("channel-missing-color", "OMERO metadata channel[" + i + "] is missing required field 'color'.");
            }
            if (channel.window == null) {
                warnOnce("channel-missing-window", "OMERO metadata channel[" + i + "] is missing required field 'window'.");
                continue;
            }
            if (channel.window.min == null) {
                warnOnce("window-missing-min", "OMERO metadata channel[" + i + "].window is missing required field 'min'.");
            }
            if (channel.window.max == null) {
                warnOnce("window-missing-max", "OMERO metadata channel[" + i + "].window is missing required field 'max'.");
            }
            if (channel.window.start == null) {
                warnOnce("window-missing-start", "OMERO metadata channel[" + i + "].window is missing required field 'start'.");
            }
            if (channel.window.end == null) {
                warnOnce("window-missing-end", "OMERO metadata channel[" + i + "].window is missing required field 'end'.");
            }
        }
    }

    private static void warnOnce(String key, String message) {
        if (WARNED.add(key)) {
            LOGGER.warning(message);
        }
    }
}
