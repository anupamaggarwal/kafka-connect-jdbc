package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.JdbcCredentials;
import io.confluent.connect.jdbc.util.JdbcCredentialsProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.sql.Array;
import java.util.Arrays;

/**
 * A {@link DatabaseDialect} for DuckDB with support for advanced types. See:
 * https://duckdb.org/docs/stable/sql/data_types/overview.html
 */
public class DuckdbDatabaseDialect extends GenericDatabaseDialect {

    // Logical names for advanced types (customize as needed for your schemas)
    public static final String UUID_LOGICAL_NAME = "org.apache.kafka.connect.data.UUID";
    public static final String ARRAY_LOGICAL_NAME = "io.confluent.connect.data.ARRAY";
    public static final String LIST_LOGICAL_NAME = "io.confluent.connect.data.LIST";
    public static final String MAP_LOGICAL_NAME = "io.confluent.connect.data.MAP";
    public static final String STRUCT_LOGICAL_NAME = "io.confluent.connect.data.STRUCT";
    public static final String UNION_LOGICAL_NAME = "io.confluent.connect.data.UNION";

    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(DuckdbDatabaseDialect.class.getSimpleName(), "duckdb");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new DuckdbDatabaseDialect(config);
        }
    }

    public DuckdbDatabaseDialect(AbstractConfig config) {
        super(config, new IdentifierRules(".", "\"", "\""));
    }

    @Override
    protected  JdbcCredentialsProvider getJdbcCredentialsProvider(AbstractConfig config) {
        return ()-> new JdbcCredentials() {
            @Override
            public String getUsername() {
                return "";
            }

            @Override
            public String getPassword() {
                return "";
            }
        };
    }


    @Override
    protected boolean includeTable(TableId table) {
        // DuckDB does not have system tables with a special prefix
        return true;
    }

    @Override
    protected String getSqlType(SinkRecordField field) {
        // Map Kafka Connect types to DuckDB types per
        // https://duckdb.org/docs/stable/sql/data_types/overview.html
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    return "DECIMAL";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                    return "TIME";
                case Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP";
                case UUID_LOGICAL_NAME:
                    return "UUID";
                case ARRAY_LOGICAL_NAME:
                case LIST_LOGICAL_NAME:
                    // Example: INTEGER[]
                    if (field.schemaParameters() != null
                            && field.schemaParameters().containsKey("elementType")) {
                        return field.schemaParameters().get("elementType") + "[]";
                    }
                    return "VARCHAR[]";
                case MAP_LOGICAL_NAME:
                    // Example: MAP(INTEGER, VARCHAR)
                    if (field.schemaParameters() != null
                            && field.schemaParameters().containsKey("keyType")
                            && field.schemaParameters().containsKey("valueType")) {
                        return "MAP("
                                + field.schemaParameters().get("keyType")
                                + ", "
                                + field.schemaParameters().get("valueType")
                                + ")";
                    }
                    return "MAP(VARCHAR, VARCHAR)";
                case STRUCT_LOGICAL_NAME:
                    // Example: STRUCT(a INTEGER, b VARCHAR)
                    // You may need to build this string from schema parameters
                    return "STRUCT"; // Fallback, see below for more details
                case UNION_LOGICAL_NAME:
                    // Example: UNION(num INTEGER, text VARCHAR)
                    return "UNION"; // Fallback, see below for more details
                default:
                    // pass through to normal types
            }
        }
        switch (field.schemaType()) {
            case BOOLEAN:
                return "BOOLEAN";
            case INT8:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INTEGER";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case STRING:
                return "VARCHAR";
            case BYTES:
                return "BLOB";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String addFieldToSchema(
            io.confluent.connect.jdbc.util.ColumnDefinition columnDefn, SchemaBuilder builder) {
        String typeName = columnDefn.typeName();
        String fieldName = columnDefn.id().aliasOrName();
        boolean optional = columnDefn.isOptional();

        // UUID
        if ("UUID".equalsIgnoreCase(typeName)) {
            builder.field(
                    fieldName, optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
            return fieldName;
        }
        // ARRAY or LIST
        if (typeName != null
                && (typeName.endsWith("[]")
                        || "ARRAY".equalsIgnoreCase(typeName)
                        || "LIST".equalsIgnoreCase(typeName))) {
            builder.field(
                    fieldName,
                    optional
                            ? SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build()
                            : SchemaBuilder.array(Schema.STRING_SCHEMA).build());
            return fieldName;
        }
        // MAP
        if ("MAP".equalsIgnoreCase(typeName)) {
            builder.field(
                    fieldName,
                    optional
                            ? SchemaBuilder.map(
                                            Schema.OPTIONAL_STRING_SCHEMA,
                                            Schema.OPTIONAL_STRING_SCHEMA)
                                    .optional()
                                    .build()
                            : SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                                    .build());
            return fieldName;
        }
        // STRUCT
        if ("STRUCT".equalsIgnoreCase(typeName)) {
            // Fallback: treat as string, or you can build a nested schema if you have the info
            builder.field(
                    fieldName, optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
            return fieldName;
        }
        // UNION
        if ("UNION".equalsIgnoreCase(typeName)) {
            // Fallback: treat as string, or you can build a union schema if you have the info
            builder.field(
                    fieldName, optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
            return fieldName;
        }
        // Fallback to default
        return super.addFieldToSchema(columnDefn, builder);
    }

    @Override
    public ColumnConverter createColumnConverter(
            io.confluent.connect.jdbc.source.ColumnMapping mapping) {
        io.confluent.connect.jdbc.util.ColumnDefinition defn = mapping.columnDefn();
        String typeName = defn.typeName();

        // UUID
        if ("UUID".equalsIgnoreCase(typeName)) {
            return rs -> rs.getString(mapping.columnNumber());
        }
        // ARRAY or LIST
        if (typeName != null
                && (typeName.endsWith("[]")
                        || "ARRAY".equalsIgnoreCase(typeName)
                        || "LIST".equalsIgnoreCase(typeName))) {
            return rs -> {
                Array array = rs.getArray(mapping.columnNumber());
                return array != null ? Arrays.asList((Object[]) array.getArray()) : null;
            };
        }
        // MAP
        if ("MAP".equalsIgnoreCase(typeName)) {
            // DuckDB returns maps as java.sql.Struct or String, depending on driver version
            return rs -> rs.getString(mapping.columnNumber());
        }
        // STRUCT
        if ("STRUCT".equalsIgnoreCase(typeName)) {
            // DuckDB returns structs as java.sql.Struct or String, depending on driver version
            return rs -> rs.getString(mapping.columnNumber());
        }
        // UNION
        if ("UNION".equalsIgnoreCase(typeName)) {
            // DuckDB returns unions as String or Object, depending on driver version
            return rs -> rs.getString(mapping.columnNumber());
        }
        // Fallback to default
        return super.createColumnConverter(mapping);
    }

    @Override
    protected String currentTimestampDatabaseQuery() {
        return "SELECT CURRENT_TIMESTAMP";
    }

    @Override
    protected String checkConnectionQuery() {
        return "SELECT 1";
    }
}
