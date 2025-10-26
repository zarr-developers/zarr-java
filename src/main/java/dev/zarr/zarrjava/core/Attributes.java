package dev.zarr.zarrjava.core;

import dev.zarr.zarrjava.ZarrException;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Attributes extends HashMap<String, Object> {

    public Attributes() {
        super();
    }

    public Attributes (Function<Attributes, Attributes> attributeMapper) {
        super();
        attributeMapper.apply(this);
    }


    public Attributes(Map<String, Object> attributes) {
        super(attributes);
    }

    public Attributes add(String s, Object o){
        this.put(s, o);
        return this;
    }

    public Attributes delete(String s){
        this.remove(s);
        return this;
    }

    public boolean getBoolean(String key) {
        Object value = this.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        throw new IllegalArgumentException("Value for key " + key + " is not a Boolean");
    }

    public int getInt(String key) {
        Object value = this.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        throw new IllegalArgumentException("Value for key " + key + " is not an Integer");
    }

    public double getDouble(String key) {
        Object value = this.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        throw new IllegalArgumentException("Value for key " + key + " is not a Double");
    }

    public float getFloat(String key) {
        Object value = this.get(key);
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        throw new IllegalArgumentException("Value for key " + key + " is not a Float");
    }

    @Nonnull
    public String getString(String key) {
        Object value = this.get(key);
        if (value instanceof String) {
            return (String) value;
        }
        throw new IllegalArgumentException("Value for key " + key + " is not a String");
    }

    @Nonnull
    public List<Object> getList(String key) {
        Object value = this.get(key);
        if (value instanceof List) {
            return (List<Object>) value;
        }
        throw new IllegalArgumentException("Value for key " + key + " is not a List");
    }

    public Attributes getAttributes(String key) {
        Object value = this.get(key);
        if (value instanceof Attributes) {
            return (Attributes) value;
        }
        if (value instanceof Map) {
            return new Attributes((Map<String, Object>) value);
        }
        throw new IllegalArgumentException("Value for key " + key + " is not an Attributes object");
    }

    public <T> T[] getArray(String key, Class<T> clazz) throws ZarrException {
    Object value = this.get(key);
    if (value instanceof Object[] && (((Object[]) value).length == 0 || clazz.isInstance(((Object[]) value)[0]) )) {
        return (T[]) value;
    }
    if (value instanceof List) {
        List<?> list = (List<?>) value;
        @SuppressWarnings("unchecked")
        T[] array = (T[]) java.lang.reflect.Array.newInstance(clazz, list.size());
        for (int i = 0; i < list.size(); i++) {
            Object elem = list.get(i);
            if (clazz.isInstance(elem)) {
                array[i] = clazz.cast(elem);
            } else {
                // Try to find a constructor that takes the element's class
                java.lang.reflect.Constructor<?> matched = null;
                for (java.lang.reflect.Constructor<?> c : clazz.getConstructors()) {
                    Class<?>[] params = c.getParameterTypes();
                    if (params.length == 1 && params[0].isAssignableFrom(elem.getClass())) {
                        matched = c;
                        break;
                    }
                }
                if (matched != null) {
                    try {
                        array[i] = (T) matched.newInstance(elem);
                    } catch (Exception e) {
                        throw new ZarrException("Failed to convert element at index " + i + " to type " + clazz.getName(), e);
                    }
                } else {
                    throw new IllegalArgumentException("Element at index " + i + " is not of type " + clazz.getName() + " and no suitable constructor found for conversion of type " + elem.getClass().getName());
                }
            }
        }
        return array;
    }
    throw new IllegalArgumentException("Value for key " + key + " is not a List or array of type " + clazz.getName());
}

    public int[] getIntArray(String key) {
        Object value = this.get(key);
        if (value instanceof int[]) {
            return (int[]) value;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            int[] array = new int[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object elem = list.get(i);
                if (elem instanceof Number) {
                    array[i] = ((Number) elem).intValue();
                } else {
                    throw new IllegalArgumentException("Element at index " + i + " is not a Number");
                }
            }
            return array;
        }
        throw new IllegalArgumentException("Value for key " + key + " is not an int array or List");
    }

    public long[] getLongArray(String key) {
        Object value = this.get(key);
        if (value instanceof long[]) {
            return (long[]) value;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            long[] array = new long[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object elem = list.get(i);
                if (elem instanceof Number) {
                    array[i] = ((Number) elem).longValue();
                } else {
                    throw new IllegalArgumentException("Element at index " + i + " is not a Number");
                }
            }
            return array;
        }
        throw new IllegalArgumentException("Value for key " + key + " is not a long array or List");
    }

    public double[] getDoubleArray(String key) {
        Object value = this.get(key);
        if (value instanceof double[]) {
            return (double[]) value;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            double[] array = new double[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object elem = list.get(i);
                if (elem instanceof Number) {
                    array[i] = ((Number) elem).doubleValue();
                } else {
                    throw new IllegalArgumentException("Element at index " + i + " is not a Number");
                }
            }
            return array;
        }
        throw new IllegalArgumentException("Value for key " + key + " is not a double array or List");
    }

    public float[] getFloatArray(String key) {
        Object value = this.get(key);
        if (value instanceof float[]) {
            return (float[]) value;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            float[] array = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object elem = list.get(i);
                if (elem instanceof Number) {
                    array[i] = ((Number) elem).floatValue();
                } else {
                    throw new IllegalArgumentException("Element at index " + i + " is not a Number");
                }
            }
            return array;
        }
        throw new IllegalArgumentException("Value for key " + key + " is not a float array or List");
    }

    public boolean[] getBooleanArray(String key) {
        Object value = this.get(key);
        if (value instanceof boolean[]) {
            return (boolean[]) value;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            boolean[] array = new boolean[list.size()];
            for (int i = 0; i < list.size(); i++) {
                Object elem = list.get(i);
                if (elem instanceof Boolean) {
                    array[i] = (Boolean) elem;
                } else {
                    throw new IllegalArgumentException("Element at index " + i + " is not a Boolean");
                }
            }
            return array;
        }
        throw new IllegalArgumentException("Value for key " + key + " is not a boolean array or List");
    }
}