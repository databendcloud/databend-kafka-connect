package com.databend.kafka.connect.util;

import org.apache.kafka.connect.data.Schema;

/**
 * General string utilities that are missing from the standard library and may commonly be
 * required by Connector or Task implementations.
 */
public class StringUtils {

    /**
     * Generate a String by appending all the @{elements}, converted to Strings, delimited by
     * {@code delim}.
     * @param elements list of elements to concatenate
     * @param delim delimiter to place between each element
     * @param <T> the type of objects to concatenate
     * @return the concatenated string with delimiters
     */
    public static <T> String join(Iterable<T> elements, String delim) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (T elem : elements) {
            if (first) {
                first = false;
            } else {
                result.append(delim);
            }
            result.append(elem);
        }
        return result.toString();
    }

    /**
     * Get a string representation of the supplied value that can be included in a log message.
     *
     * @param value the value; may be null
     * @return the loggable string representation
     */
    public static String valueTypeOrNull(Object value) {
        return value == null ? null : value.getClass().getSimpleName();
    }

    /**
     * Get a string representation of the supplied schema that can be included in a log message.
     *
     * @param schema the schema; may be null
     * @return the loggable string representation
     */
    public static String schemaTypeOrNull(Schema schema) {
        if (schema == null) {
            return null;
        }
        switch (schema.type()) {
            case STRUCT:
                return "Struct";
            default:
                return schema.type().getName();
        }
    }
}

