package dev.zarr.zarrjava.experimental.ome;

import com.fasterxml.jackson.core.type.TypeReference;
import dev.zarr.zarrjava.ZarrException;
import dev.zarr.zarrjava.core.Attributes;
import dev.zarr.zarrjava.store.StoreHandle;
import dev.zarr.zarrjava.v2.Group;
import dev.zarr.zarrjava.v2.GroupMetadata;

import javax.annotation.Nonnull;

/**
 * Base class for all OME-Zarr nodes backed by a Zarr v2 group.
 *
 * <p>Provides {@code protected static} helpers for reading attributes and building
 * {@link Attributes} for writing. The actual byte serialization is performed by
 * {@link dev.zarr.zarrjava.v2.Node#makeObjectWriter()} inside {@code Group.create()} and
 * {@code Group.setAttributes()}.
 */
public abstract class OmeV2Group extends Group {

    protected OmeV2Group(@Nonnull StoreHandle storeHandle, @Nonnull GroupMetadata groupMetadata) {
        super(storeHandle, groupMetadata);
    }

    /** Reads and converts a named attribute value from the given v2 group's attributes. */
    protected static <T> T readAttribute(
            Attributes attributes, StoreHandle storeHandle, String key, Class<T> cls)
            throws ZarrException {
        if (attributes == null || !attributes.containsKey(key)) {
            throw new ZarrException("No '" + key + "' key found in attributes at " + storeHandle);
        }
        return OmeObjectMappers.makeV2Mapper().convertValue(attributes.get(key), cls);
    }

    /** Reads and converts a named attribute using a {@link TypeReference} (e.g. for {@code List<T>}). */
    protected static <T> T readTypedAttribute(
            Attributes attributes, StoreHandle storeHandle, String key, TypeReference<T> typeRef)
            throws ZarrException {
        if (attributes == null || !attributes.containsKey(key)) {
            throw new ZarrException("No '" + key + "' key found in attributes at " + storeHandle);
        }
        return OmeObjectMappers.makeV2Mapper().convertValue(attributes.get(key), typeRef);
    }

    /**
     * Builds {@link Attributes} containing {@code {key: <serialized value>}}, ready to
     * pass to {@code Group.create()} or {@code Group.setAttributes()}.
     */
    protected static Attributes buildAttributes(String key, Object value) {
        Object serialized = dev.zarr.zarrjava.v2.Node.makeObjectMapper()
                .convertValue(value, Object.class);
        Attributes attrs = new Attributes();
        attrs.put(key, serialized);
        return attrs;
    }

    /** Serializes {@code value} via the v2 mapper to a plain Java object (Map/List/primitive). */
    protected static Object serialize(Object value) {
        return dev.zarr.zarrjava.v2.Node.makeObjectMapper().convertValue(value, Object.class);
    }
}
