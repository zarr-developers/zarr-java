package dev.zarr.zarrjava.experimental.ome;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

final class OmeObjectMappers {
    private OmeObjectMappers() {
    }

    static ObjectMapper makeV2Mapper() {
        ObjectMapper mapper = dev.zarr.zarrjava.v2.Node.makeObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        mapper.addHandler(new UnknownOmePropertyWarningHandler());
        return mapper;
    }

    static ObjectMapper makeV3Mapper() {
        ObjectMapper mapper = dev.zarr.zarrjava.v3.Node.makeObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
        mapper.addHandler(new UnknownOmePropertyWarningHandler());
        return mapper;
    }

    private static final class UnknownOmePropertyWarningHandler extends DeserializationProblemHandler {
        private static final Logger LOGGER = Logger.getLogger(UnknownOmePropertyWarningHandler.class.getName());
        private static final Set<String> WARNED_FIELDS = ConcurrentHashMap.newKeySet();

        @Override
        public boolean handleUnknownProperty(
                DeserializationContext ctxt,
                JsonParser p,
                JsonDeserializer<?> deserializer,
                Object beanOrClass,
                String propertyName
        ) throws IOException {
            String target = (beanOrClass instanceof Class)
                    ? ((Class<?>) beanOrClass).getName()
                    : beanOrClass.getClass().getName();
            String key = target + "#" + propertyName;
            if (WARNED_FIELDS.add(key)) {
                LOGGER.warning(
                        "Ignoring unknown OME metadata field '" + propertyName + "' for " + target);
            }
            p.skipChildren();
            return true;
        }
    }
}
