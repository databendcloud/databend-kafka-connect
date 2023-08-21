package com.databend.kafka.connect;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.Config;

import static com.databend.kafka.connect.sink.DatabendSinkConfig.*;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

public class DatabendSinkConnectorTest {
    @Test
    public void testValidationWhenDeleteEnabled() {

        DatabendSinkConnector connector = new DatabendSinkConnector();

        Map<String, String> connConfig = new HashMap<>();
        connConfig.put("connector.class", "com.databend.kafka.connect.DatabendSinkConnector");
        connConfig.put("delete.enabled", "true");
        final String conflictMsg = "Deletes are only supported when pk_fields not empty";

        assertEquals("'pk_fields' must not empty when 'delete.enabled' == true",
                singletonList(conflictMsg),
                configErrors(connector.validate(connConfig), DELETE_ENABLED));


        connConfig.put("pk.fields", "k1,k2");
        assertEquals("'pk.fields not empty when 'delete.enabled' == true",
                EMPTY_LIST, configErrors(connector.validate(connConfig), DELETE_ENABLED));
    }

    @Test
    public void testValidationWhenDeleteNotEnabled() {

        DatabendSinkConnector connector = new DatabendSinkConnector();

        Map<String, String> connConfig = new HashMap<>();
        connConfig.put("connector.class", "com.databend.kafka.connect.DatabendSinkConnector");
        connConfig.put("delete.enabled", "false");

        assertEquals("empty pk.fields is valid when 'delete.enabled' == false",
                EMPTY_LIST, configErrors(connector.validate(connConfig), DELETE_ENABLED));
    }

    private List<String> configErrors(Config config, String propertyName) {
        return config.configValues()
                .stream()
                .flatMap(cfg -> propertyName.equals(cfg.name()) ?
                        cfg.errorMessages().stream() : Stream.empty())
                .collect(Collectors.toList());
    }

    private Optional<ConfigValue> configValue(Config config, String name) {
        return config.configValues()
                .stream()
                .filter(cfg -> name.equals(cfg.name())
                        && cfg.errorMessages().isEmpty())
                .findFirst();
    }
}