package com.databend.kafka.connect.databendclient;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class CachedConnectionProvider implements ConnectionProvider {

    private static final Logger log = LoggerFactory.getLogger(CachedConnectionProvider.class);

    private static final int VALIDITY_CHECK_TIMEOUT_S = 5;

    private final ConnectionProvider provider;
    private final int maxConnectionAttempts;
    private final long connectionRetryBackoff;

    private int count = 0;
    private Connection connection;
    private volatile boolean isRunning = true;

    public CachedConnectionProvider(
            ConnectionProvider provider,
            int maxConnectionAttempts,
            long connectionRetryBackoff
    ) {
        this.provider = provider;
        this.maxConnectionAttempts = maxConnectionAttempts;
        this.connectionRetryBackoff = connectionRetryBackoff;
    }

    @Override
    public synchronized Connection getConnection() {
        log.debug("Trying to establish connection with the database.");
        try {
            if (connection == null) {
                newConnection();
            } else if (!isConnectionValid(connection, VALIDITY_CHECK_TIMEOUT_S)) {
                log.info("The database connection is invalid. Reconnecting...");
                close();
                newConnection();
            }
        } catch (SQLException sqle) {
            log.debug("Could not establish connection with database.", sqle);
            throw new ConnectException(sqle);
        }
        log.debug("Database connection established.");
        return connection;
    }

    @Override
    public boolean isConnectionValid(Connection connection, int timeout) {
        try {
            return provider.isConnectionValid(connection, timeout);
        } catch (SQLException sqle) {
            log.debug("Unable to check if the underlying connection is valid", sqle);
            return false;
        }
    }

    private void newConnection() throws SQLException {
        int attempts = 0;
        while (isRunning) {
            try {
                ++count;
                log.debug("Attempting to open connection #{} to {}", count, provider);
                connection = provider.getConnection();
                onConnect(connection);
                return;
            } catch (SQLException sqle) {
                attempts++;
                if (isRunning && attempts < maxConnectionAttempts) {
                    log.info("Unable to connect to database on attempt {}/{}. Will retry in {} ms.", attempts,
                            maxConnectionAttempts, connectionRetryBackoff, sqle
                    );
                    try {
                        Thread.sleep(connectionRetryBackoff);
                    } catch (InterruptedException e) {
                        // this is ok because just woke up early
                    }
                } else {
                    throw sqle;
                }
            }
        }
    }

    public void close(boolean stopping) {
        isRunning = !stopping;
        close();
    }

    @Override
    public synchronized void close() {
        if (connection != null) {
            try {
                log.debug("Closing connection #{} to {}", count, provider);
                connection.close();
            } catch (SQLException sqle) {
                log.warn("Ignoring error closing connection", sqle);
            } finally {
                connection = null;
                provider.close();
            }
        }
    }

    @Override
    public String identifier() {
        return provider.identifier();
    }

    protected void onConnect(Connection connection) throws SQLException {
    }

}

