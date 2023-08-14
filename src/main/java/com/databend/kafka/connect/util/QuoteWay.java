package com.databend.kafka.connect.util;

public enum QuoteWay {
    ALWAYS("always"),
    NEVER("never");

    public static QuoteWay get(String name) {
        for (QuoteWay method : values()) {
            if (method.toString().equalsIgnoreCase(name)) {
                return method;
            }
        }
        throw new IllegalArgumentException("No matching QuoteMethod found for '" + name + "'");
    }

    private final String name;

    QuoteWay(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}

