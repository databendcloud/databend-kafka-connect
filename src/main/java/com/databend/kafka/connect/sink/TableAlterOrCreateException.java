package com.databend.kafka.connect.sink;

import org.apache.kafka.connect.errors.ConnectException;

public class TableAlterOrCreateException extends ConnectException {
    private static final long serialVersionUID = 1L;
    public TableAlterOrCreateException(String reason) {
        super(reason);
    }

    public TableAlterOrCreateException(String reason, Throwable throwable) {
        super(reason, throwable);
    }
}
