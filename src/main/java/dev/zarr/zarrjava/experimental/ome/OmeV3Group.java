package dev.zarr.zarrjava.experimental.ome;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v3.Group;
import dev.zarr.zarrjava.v3.GroupMetadata;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Base class for all OME-Zarr nodes backed by a Zarr v3 group.
 *
 * <p>Provides {@code protected static} helpers for reading OME attributes and building
 * {@link Attributes} for writing. The actual byte serialization is performed by
 * {@link dev.zarr.zarrjava.v3.Node#makeObjectWriter()} inside {@code Group.create()} and
 * {@code Group.setAttributes()}.
 */
public abstract class OmeV3Group extends Group {

    protected OmeV3Group(@Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata)
            throws IOException {
        super(storeHandle, groupMetadata);
    }

    /** Reads and converts the {@code "ome"} attribute value from the given group's attributes. */
    protected static <T> T readOmeAttribute(
            Attributes attributes, StoreHandle storeHandle, Class<T> cls) throws ZarrException {
        if (attributes == null || !attributes.containsKey("ome")) {
            throw new ZarrException("No 'ome' key found in attributes at " + storeHandle);
        }
        return OmeObjectMappers.makeV3Mapper().convertValue(attributes.get("ome"), cls);
    }

    /**
     * Builds {@link Attributes} containing {@code {"ome": <serialized omeMetadata>}}, ready to
     * pass to {@code Group.create()} or {@code Group.setAttributes()}.
     */
    protected static Attributes omeAttributes(Object omeMetadata) {
        Object serialized = dev.zarr.zarrjava.v3.Node.makeObjectMapper()
                .convertValue(omeMetadata, Object.class);
        Attributes attrs = new Attributes();
        attrs.put("ome", serialized);
        return attrs;
    }
}
