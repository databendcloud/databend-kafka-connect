package com.databend.kafka.connect.sink.records;

import com.databend.jdbc.com.fasterxml.jackson.databind.JsonNode;
import com.databend.kafka.connect.sink.DatabendSinkConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabendAvroConverter extends DatabendConverter {
    private SchemaRegistryClient schemaRegistry = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabendSinkConfig.class);
    public static final String BREAK_ON_SCHEMA_REGISTRY_ERROR = "break.on.schema.registry.error";
    public static final String READER_SCHEMA = "reader.schema";

    // By default, we don't break when schema registry is not found
    private boolean breakOnSchemaRegistryError = false;
    /* By default, no reader schema is set. In this case, the writer schema of each item is also used
    as the reader schema. See https://avro.apache.org/docs/1.9.2/spec.html#Schema+Resolution */
    private Schema readerSchema = null;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        readBreakOnSchemaRegistryError(configs);
        parseReaderSchema(configs);
        try {
            AvroConverterConfig avroConverterConfig = new AvroConverterConfig(configs);
            schemaRegistry =
                    new CachedSchemaRegistryClient(
                            avroConverterConfig.getSchemaRegistryUrls(),
                            avroConverterConfig.getMaxSchemasPerSubject(),
                            configs);
        } catch (Exception e) {
            throw e;
        }
    }

    void readBreakOnSchemaRegistryError(final Map<String, ?> configs) {
        try {
            Object shouldBreak = configs.get(BREAK_ON_SCHEMA_REGISTRY_ERROR);
            if (shouldBreak instanceof String) {
                breakOnSchemaRegistryError = ((String) shouldBreak).toLowerCase().equals("true");
            }
        } catch (Exception e) {
            // do nothing
        }
    }

    /**
     * Parse reader schema from config if provided
     *
     * @param configs configuration for converter
     */
    void parseReaderSchema(final Map<String, ?> configs) {
        Object readerSchemaFromConfig = configs.get(READER_SCHEMA);

        if (readerSchemaFromConfig == null) {
            return;
        }

        if (readerSchemaFromConfig instanceof String) {
            try {
                readerSchema = new Schema.Parser().parse((String) readerSchemaFromConfig);
            } catch (SchemaParseException e) {
                LOGGER.error(
                        "the string provided for reader.schema is no valid Avro schema: " + e.getMessage());
                throw e;
            }
        } else {
            LOGGER.error("reader.schema has to be a string");
        }
    }

    // for testing only
    boolean getBreakOnSchemaRegistryError() {
        return breakOnSchemaRegistryError;
    }

    /**
     * set a schema registry client for test use only
     *
     * @param schemaRegistryClient mock schema registry client
     */
    void setSchemaRegistry(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistry = schemaRegistryClient;
    }
    public String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }
    /**
     * cast bytes array to JsonNode array
     *
     * @param s     topic, unused
     * @param bytes input bytes array
     * @return JsonNode array
     */
    @Override
    public SchemaAndValue toConnectData(final String s, final byte[] bytes) {
        if (bytes == null) {
            return new SchemaAndValue(new DatabendJsonSchema(), new DatabendRecordContent());
        }
        ByteBuffer buffer;
        int id;
        try {
            buffer = ByteBuffer.wrap(bytes);
            if (buffer.get() != 0) {
                throw new IOException("Unknown magic byte!");
            }
            id = buffer.getInt();
        } catch (Exception e) {
            return logErrorAndReturnBrokenRecord(e, bytes);
        }
        LOGGER.info("schema sjh: {}", s);
        LOGGER.info("schema bytes str: {}", bytesToString(bytes));

        // If there is any error while getting writer schema from schema registry,
        // throw error and break the connector
        Schema writerSchema;
        try {
            writerSchema = schemaRegistry.getById(id);
            LOGGER.info("schema: {}", writerSchema.getFields());
        } catch (Exception e) {
            return logErrorAndReturnBrokenRecord(e, bytes);
        }

        try {
            int length = buffer.limit() - 1 - 4;
            byte[] data = new byte[length];
            buffer.get(data, 0, length);

            return new SchemaAndValue(
                    new DatabendJsonSchema(),
                    new DatabendRecordContent(
                            parseAvroWithSchema(
                                    data, writerSchema, readerSchema == null ? writerSchema : readerSchema),
                            id));
        } catch (Exception e) {

            return logErrorAndReturnBrokenRecord(e, bytes);
        }
    }

    private SchemaAndValue logErrorAndReturnBrokenRecord(final Exception e, final byte[] bytes) {

        LOGGER.error("failed to parse AVRO record\n" + e.getMessage());
        return new SchemaAndValue(new DatabendJsonSchema(), new DatabendRecordContent(bytes));
    }

    /**
     * Parse Avro record with a writer schema and a reader schema. The writer and the reader schema
     * have to be compatible as described in
     * https://avro.apache.org/docs/1.9.2/spec.html#Schema+Resolution
     *
     * @param data         avro data
     * @param writerSchema avro schema with which data got serialized
     * @param readerSchema avro schema that describes the shape of the returned JsonNode
     * @return JsonNode array
     */
    private JsonNode parseAvroWithSchema(final byte[] data, Schema writerSchema, Schema readerSchema)
            throws IOException {
        final GenericData genericData = new GenericData();
        // Conversion for logical type Decimal. There are conversions for other logical types as well.
        genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());

        InputStream is = new ByteArrayInputStream(data);
        Decoder decoder = DecoderFactory.get().binaryDecoder(is, null);
        DatumReader<GenericRecord> reader =
                new GenericDatumReader<>(writerSchema, readerSchema, genericData);
        GenericRecord datum = reader.read(null, decoder);
        // For byte data without logical type, this toString method handles it this way:
        // writeEscapedString(StandardCharsets.ISO_8859_1.decode(bytes), buffer);
        // The generated string is escaped ISO_8859_1 decoded string.
        LOGGER.info("datum string: {}", datum.toString()); // parse ok
        return mapper.readTree(datum.toString());
    }
}

