package com.databend.kafka.connect.databendclient;

import com.databend.kafka.connect.util.QuoteWay;

import java.util.Objects;

public class ColumnIdentity implements SQLExpressionBuilder.Expressable {

    private final TableIdentity tableId;
    private final String name;
    private final String alias;
    private final int hash;

    public ColumnIdentity(
            TableIdentity tableId,
            String columnName
    ) {
        this(tableId, columnName, null);
    }

    public ColumnIdentity(
            TableIdentity tableId,
            String columnName,
            String alias
    ) {
        assert columnName != null;
        this.tableId = tableId;
        this.name = columnName;
        this.alias = alias != null && !alias.trim().isEmpty() ? alias : name;
        this.hash = Objects.hash(this.tableId, this.name);
    }

    public TableIdentity tableId() {
        return tableId;
    }

    public String name() {
        return name;
    }

    /**
     * Gets the column's suggested title for use in printouts and displays. The suggested title is
     * usually specified by the SQL <code>AS</code> clause.  If a SQL <code>AS</code> is not
     * specified, the value will be the same as the value returned by the {@link #name()} method.
     *
     * @return the suggested column title; never null
     */
    public String aliasOrName() {
        return alias;
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
        if (tableId != null) {
            builder.append(tableId);
            builder.appendIdentifierDelimiter();
        }
        builder.appendColumnName(this.name, useQuotes);
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
        if (obj instanceof ColumnIdentity) {
            ColumnIdentity that = (ColumnIdentity) obj;
            return Objects.equals(this.name, that.name) && Objects.equals(this.alias, that.alias)
                    && Objects.equals(this.tableId, that.tableId);
        }
        return false;
    }

    @Override
    public String toString() {
        return SQLExpressionBuilder.create().append(this).toString();
    }
}

