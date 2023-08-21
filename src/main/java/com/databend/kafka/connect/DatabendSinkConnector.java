package com.databend.kafka.connect;

import java.util.Optional;

import com.databend.kafka.connect.sink.DatabendSinkConfig;
import com.databend.kafka.connect.sink.DatabendSinkTask;
import com.databend.kafka.connect.util.Version;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.databend.kafka.connect.sink.DatabendSinkConfig.PK_FIELDS;
import static com.databend.kafka.connect.sink.DatabendSinkConfig.DELETE_ENABLED;

public class DatabendSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(DatabendSinkConnector.class);

    private Map<String, String> configProps;

    public Class<? extends Task> taskClass() {
        return DatabendSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return DatabendSinkConfig.CONFIG_DEF;
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        /** get configuration parsed and validated individually */
        Config config = super.validate(connectorConfigs);

        return validateDeleteEnabledPkFields(config);
    }

    private Config validateDeleteEnabledPkFields(Config config) {

        if (configValue(config, PK_FIELDS).isPresent() && !configValue(config, PK_FIELDS).get().value().toString().replace("[]", "").isEmpty()) {
            return config;
        } else {
            configValue(config, DELETE_ENABLED)
                    .filter(deleteEnabled -> Boolean.TRUE.equals(deleteEnabled.value()))
                    .ifPresent(deleteEnabled -> {
                        String conflictMsg = "Deletes are only supported when pk_fields not empty";
                        deleteEnabled.addErrorMessage(conflictMsg);
                    });

            List<ConfigValue> configValues = config.configValues();
            for (ConfigValue cfg : configValues) {
                if (DELETE_ENABLED.equals(cfg.name())) {
                    cfg.value("false");
                    break;
                }
            }
        }

        return config;
    }

    /**
     * only if individual validation passed.
     */
    private Optional<ConfigValue> configValue(Config config, String name) {
        return config.configValues()
                .stream()
                .filter(cfg -> name.equals(cfg.name())
                        && cfg.errorMessages().isEmpty())
                .findFirst();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}