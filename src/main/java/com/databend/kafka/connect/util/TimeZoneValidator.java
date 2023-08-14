package com.databend.kafka.connect.util;

import java.util.Arrays;
import java.util.TimeZone;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class TimeZoneValidator implements ConfigDef.Validator {

    public static final TimeZoneValidator INSTANCE = new TimeZoneValidator();

    @Override
    public void ensureValid(String name, Object value) {
        if (value != null) {
            if (!Arrays.asList(TimeZone.getAvailableIDs()).contains(value.toString())) {
                throw new ConfigException(name, value, "Invalid time zone identifier");
            }
        }
    }

    @Override
    public String toString() {
        return "Any valid JDK time zone";
    }
}

