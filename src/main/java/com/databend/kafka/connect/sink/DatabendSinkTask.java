package com.databend.kafka.connect.sink;

import com.databend.kafka.connect.databendclient.DatabendConnection;
import com.databend.kafka.connect.util.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class DatabendSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(DatabendSinkConfig.class);

    ErrantRecordReporter reporter;
    DatabendConnection dbConnection;
    DatabendSinkConfig config;
    DatabendWriter writer;
    int remainingRetries;

    boolean shouldTrimSensitiveLogs;

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting Databend Sink task");
        config = new DatabendSinkConfig(props);
        initWriter();
        remainingRetries = config.maxRetries;
        shouldTrimSensitiveLogs = config.trimSensitiveLogsEnabled;
        try {
            reporter = context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            reporter = null;
        }
    }

    void initWriter() {
        log.info("Initializing Databend writer");

        dbConnection = DatabendClient.create(config);

        final DbStructure dbStructure = new DbStructure(dbConnection);
        log.info("Initializing writer using SQL dialect: {}", dbConnection.getClass().getSimpleName());
        writer = new DatabendWriter(config, dbConnection, dbStructure);
        log.info("Databend writer initialized");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.debug(
                "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                        + "database...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );
        try {
            writer.write(records);
        } catch (TableAlterOrCreateException tace) {
            if (reporter != null) {
                unrollAndRetry(records);
            } else {
                log.error(tace.toString());
                throw tace;
            }
        } catch (SQLException sqle) {
            log.warn(
                    "Write of {} records failed, remainingRetries={}",
                    records.size(),
                    remainingRetries
            );
            int totalExceptions = 0;
            for (Throwable e : sqle) {
                totalExceptions++;
            }
            SQLException sqlAllMessagesException = getAllMessagesException(sqle);
            if (remainingRetries > 0) {
                writer.closeQuietly();
                initWriter();
                remainingRetries--;
                context.timeout(config.retryBackoffMs);
                log.debug(sqlAllMessagesException.toString());
                throw new RetriableException(sqlAllMessagesException);
            } else {
                if (reporter != null) {
                    unrollAndRetry(records);
                } else {
                    log.error(
                            "Failing task after exhausting retries; "
                                    + "encountered {} exceptions on last write attempt. "
                                    + "For complete details on each exception, please enable DEBUG logging.",
                            totalExceptions);
                    throw new ConnectException(sqlAllMessagesException);
                }
            }
        }
        remainingRetries = config.maxRetries;
    }

    private void unrollAndRetry(Collection<SinkRecord> records) {
        writer.closeQuietly();
        initWriter();
        for (SinkRecord record : records) {
            try {
                writer.write(Collections.singletonList(record));
            } catch (TableAlterOrCreateException tace) {
                log.debug(tace.toString());
                reporter.report(record, tace);
                writer.closeQuietly();
            } catch (SQLException sqle) {
                SQLException sqlAllMessagesException = getAllMessagesException(sqle);
                log.debug(sqlAllMessagesException.toString());
                reporter.report(record, sqlAllMessagesException);
                writer.closeQuietly();
            }
        }
    }

    private SQLException getAllMessagesException(SQLException sqle) {
        String sqleAllMessages = "Exception chain:" + System.lineSeparator();
        for (Throwable e : sqle) {
            sqleAllMessages += e + System.lineSeparator();
        }
        SQLException sqlAllMessagesException = new SQLException(sqleAllMessages);
        sqlAllMessagesException.setNextException(sqle);
        return sqlAllMessagesException;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // Not necessary
    }

    public void stop() {
        log.info("Stopping task");
        try {
            writer.closeQuietly();
        } finally {
            try {
                if (dbConnection != null) {
                    dbConnection.close();
                }
            } catch (Throwable t) {
                log.warn("Error while closing the Databend connection: ", t);
            } finally {
                dbConnection = null;
            }
        }
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}

