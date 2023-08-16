package com.databend.kafka.connect.databendclient;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum TableType {

    TABLE("TABLE", "Table"),
    VIEW("VIEW", "View");

    private final String value;
    private final String capitalCase;

    TableType(String value, String capitalCase) {
        this.value = value.toUpperCase();
        this.capitalCase = capitalCase;
    }

    public String capitalized() {
        return capitalCase;
    }

    @Override
    public String toString() {
        return value;
    }

    public static TableType get(String name) {
        if (name != null) {
            name = name.trim();
        }
        for (TableType method : values()) {
            if (method.toString().equalsIgnoreCase(name)) {
                return method;
            }
        }
        throw new IllegalArgumentException("No matching QuoteMethod found for '" + name + "'");
    }

    public static EnumSet<TableType> parse(Collection<String> values) {
        Set<TableType> types = values.stream().map(TableType::get).collect(Collectors.toSet());
        return EnumSet.copyOf(types);
    }


}
