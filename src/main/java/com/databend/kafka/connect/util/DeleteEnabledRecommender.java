package com.databend.kafka.connect.util;

import com.databend.kafka.connect.sink.DatabendSinkConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.*;

import static com.databend.kafka.connect.sink.DatabendSinkConfig.PK_FIELDS;

public class DeleteEnabledRecommender implements ConfigDef.Recommender {

    public static final DeleteEnabledRecommender INSTANCE = new DeleteEnabledRecommender();

    private static final List<Object> ALL_VALUES = Arrays.asList(Boolean.TRUE, Boolean.FALSE);
    private static final List<Object> DISABLED = Collections.singletonList(Boolean.FALSE);

    @Override
    public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
        return isRecordKeyPKMode(parsedConfig) ? ALL_VALUES : DISABLED;
    }

    @Override
    public boolean visible(final String name, final Map<String, Object> parsedConfig) {
        return isRecordKeyPKMode(parsedConfig);
    }

    private static boolean isRecordKeyPKMode(final Map<String, Object> parsedConfig) {
        return !Objects.equals(String.valueOf(parsedConfig.get(PK_FIELDS)), "");
    }

}
