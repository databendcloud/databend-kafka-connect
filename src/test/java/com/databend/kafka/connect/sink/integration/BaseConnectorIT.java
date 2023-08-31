package com.databend.kafka.connect.sink.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.NoRetryException;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.connect.runtime.ConnectorConfig.*;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public abstract class BaseConnectorIT {

    public static final String DLQ_TOPIC_NAME = "dlq-topic";

    private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

    protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(10);
    protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
    protected static final long OFFSET_COMMIT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);
    protected static final long OFFSETS_READ_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);

    private Admin kafkaAdminClient;

    protected EmbeddedConnectCluster connect;

    protected void startConnect() {
        connect = new EmbeddedConnectCluster.Builder()
                .name("databend-connect-cluster")
                .build();

        // start the clusters
        connect.start();

        kafkaAdminClient = connect.kafka().createAdminClient();
    }

    protected JsonConverter jsonConverter() {
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap(
                ConverterConfig.TYPE_CONFIG,
                ConverterType.VALUE.getName()
        ));

        return jsonConverter;
    }

    protected Map<String, String> baseSinkProps() {
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, "DatabendSinkConnector");
        // converters
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        // license properties
        props.put("confluent.topic.bootstrap.servers", connect.kafka().bootstrapServers());
        props.put("confluent.topic.replication.factor", "1");
        return props;
    }

    protected void stopConnect() throws SQLException {
        Connection connection = DriverManager.getConnection(
                "jdbc:databend://localhost:8000",
                "databend",
                "databend");
        try (Statement s = connection.createStatement()) {
            s.execute("select 1");
        }
        if (kafkaAdminClient != null) {
            kafkaAdminClient.close();
            kafkaAdminClient = null;
        }

        // stop all Connect, Kafka and Zk threads.
        if (connect != null) {
            connect.stop();
        }
    }

    /**
     * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
     * name to start the specified number of tasks.
     *
     * @param name     the name of the connector
     * @param numTasks the minimum number of tasks that are expected
     * @return the time this method discovered the connector has started, in milliseconds past epoch
     * @throws InterruptedException if this was interrupted
     */
    protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
        waitForCondition(
                () -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
                CONNECTOR_STARTUP_DURATION_MS,
                "Connector tasks did not start in time."
        );
        return System.currentTimeMillis();
    }

    /**
     * Confirm that a connector with an exact number of tasks is running.
     *
     * @param connectorName the connector
     * @param numTasks      the minimum number of tasks
     * @return true if the connector and tasks are in RUNNING state; false otherwise
     */
    protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
        try {
            ConnectorStateInfo info = connect.connectorStatus(connectorName);
            boolean result = info != null
                    && info.tasks().size() >= numTasks
                    && info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
                    && info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
            return Optional.of(result);
        } catch (Exception e) {
            log.warn("Could not check connector state info.");
            return Optional.empty();
        }
    }

    protected void waitForCommittedRecords(
            String connector, Collection<String> topics, long numRecords, int numTasks, long timeoutMs
    ) throws InterruptedException {
        waitForCondition(
                () -> {
                    long totalCommittedRecords = totalCommittedRecords(connector, topics);
                    if (totalCommittedRecords >= numRecords) {
                        return true;
                    } else {
                        // Check to make sure the connector is still running. If not, fail fast
                        try {
                            assertTrue(
                                    "Connector or one of its tasks failed during testing",
                                    assertConnectorAndTasksRunning(connector, numTasks).orElse(false));
                        } catch (AssertionError e) {
                            throw new NoRetryException(e);
                        }
                        log.debug("Connector has only committed {} records for topics {} so far; {} " +
                                        "expected",
                                totalCommittedRecords, topics, numRecords);
                        // Sleep here so as not to spam Kafka with list-offsets requests
                        Thread.sleep(OFFSET_COMMIT_INTERVAL_MS / 2);
                        return false;
                    }
                },
                timeoutMs,
                "Either the connector failed, or the message commit duration expired without all expected messages committed");
    }

    protected synchronized long totalCommittedRecords(String connector, Collection<String> topics) throws TimeoutException, ExecutionException, InterruptedException {
        // See https://github.com/apache/kafka/blob/f7c38d83c727310f4b0678886ba410ae2fae9379/connect/runtime/src/main/java/org/apache/kafka/connect/util/SinkUtils.java
        // for how the consumer group ID is constructed for sink connectors
        Map<TopicPartition, OffsetAndMetadata> offsets = kafkaAdminClient
                .listConsumerGroupOffsets("connect-" + connector)
                .partitionsToOffsetAndMetadata()
                .get(OFFSETS_READ_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        log.trace("Connector {} has so far committed offsets {}", connector, offsets);

        return offsets.entrySet().stream()
                .filter(entry -> topics.contains(entry.getKey().topic()))
                .mapToLong(entry -> entry.getValue().offset())
                .sum();
    }


}
