package com.databend.kafka.connect


import java.util.Locale;
import java.util.Optional;

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

public class DatabendSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(JdbcSinkConnector.class);

    private Map<String, String> configProps;
}