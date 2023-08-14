package com.databend.kafka.connect.databendclient;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A description of a table.
 */
public class TableDefinition {
    private final TableIdentity id;
    private final Map<String, ColumnDefinition> columnsByName = new LinkedHashMap<>();
    private final Map<String, String> pkColumnNames = new LinkedHashMap<>();
    private final TableType type;

    public TableDefinition(
            TableIdentity id,
            Iterable<ColumnDefinition> columns
    ) {
        this(id, columns, TableType.TABLE);
    }

    public TableDefinition(
            TableIdentity id,
            Iterable<ColumnDefinition> columns,
            TableType type
    ) {
        this.id = id;
        this.type = Objects.requireNonNull(type);
        for (ColumnDefinition defn : columns) {
            String columnName = defn.id().name();
            columnsByName.put(
                    columnName,
                    defn.forTable(this.id)
            );
            if (defn.isPrimaryKey()) {
                this.pkColumnNames.put(
                        columnName,
                        columnName
                );
            }
        }
    }

    public TableIdentity id() {
        return id;
    }

    public TableType type() {
        return type;
    }

    public int columnCount() {
        return columnsByName.size();
    }

    public ColumnDefinition definitionForColumn(String name) {
        return columnsByName.get(name);
    }

    public Collection<ColumnDefinition> definitionsForColumns() {
        return columnsByName.values();
    }

    public Collection<String> primaryKeyColumnNames() {
        return pkColumnNames.values();
    }

    public Set<String> columnNames() {
        return columnsByName.keySet();
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return true;
        }
        if (obj instanceof TableDefinition) {
            TableDefinition that = (TableDefinition) obj;
            return Objects.equals(this.id(), that.id())
                    && Objects.equals(this.type(), that.type())
                    && Objects.equals(this.definitionsForColumns(), that.definitionsForColumns());
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format(
                "Table{name='%s', type=%s columns=%s}",
                id,
                type,
                definitionsForColumns()
        );
    }
}

