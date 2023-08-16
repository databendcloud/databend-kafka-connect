package com.databend.kafka.connect.databendclient;

import com.databend.kafka.connect.util.QuoteWay;

import java.util.Objects;


public class TableIdentity implements Comparable<TableIdentity>, SQLExpressionBuilder.Expressable {

    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final int hash;

    public TableIdentity(
            String catalogName,
            String schemaName,
            String tableName
    ) {
        this.catalogName = catalogName == null || catalogName.isEmpty() ? null : catalogName;
        this.schemaName = schemaName == null || schemaName.isEmpty() ? null : schemaName;
        this.tableName = tableName;
        this.hash = Objects.hash(catalogName, schemaName, tableName);
    }

    public String catalogName() {
        return catalogName;
    }

    public String schemaName() {
        return schemaName;
    }

    public String tableName() {
        return tableName;
    }

    @Override
    public void appendTo(SQLExpressionBuilder builder, boolean useQuotes) {
        appendTo(builder, useQuotes ? QuoteWay.ALWAYS : QuoteWay.NEVER);
    }

    @Override
    public void appendTo(
            SQLExpressionBuilder builder,
            QuoteWay useQuotes
    ) {
        if (catalogName != null) {
            builder.appendIdentifier(catalogName, useQuotes);
            builder.appendIdentifierDelimiter();
        }
        if (schemaName != null) {
            builder.appendIdentifier(schemaName, useQuotes);
            builder.appendIdentifierDelimiter();
        }
        builder.appendTableName(tableName, useQuotes);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof TableIdentity) {
            TableIdentity that = (TableIdentity) obj;
            return Objects.equals(this.catalogName, that.catalogName)
                    && Objects.equals(this.schemaName, that.schemaName)
                    && Objects.equals(this.tableName, that.tableName);
        }
        return false;
    }

    @Override
    public int compareTo(TableIdentity that) {
        if (that == this) {
            return 0;
        }
        int diff = this.tableName.compareTo(that.tableName);
        if (diff != 0) {
            return diff;
        }
        if (this.schemaName == null) {
            if (that.schemaName != null) {
                return -1;
            }
        } else {
            if (that.schemaName == null) {
                return 1;
            }
            diff = this.schemaName.compareTo(that.schemaName);
            if (diff != 0) {
                return diff;
            }
        }
        if (this.catalogName == null) {
            if (that.catalogName != null) {
                return -1;
            }
        } else {
            if (that.catalogName == null) {
                return 1;
            }
            diff = this.catalogName.compareTo(that.catalogName);
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        return SQLExpressionBuilder.create().append(this).toString();
    }
}
