package datamigration.utils;

public enum Type {
    INTEGER,
    LONG,
    FLOAT,
    TEXT,
    STRING,
    DATE,
    ENUM,
    UNKNOWN;

    static String ENUM_PREFIX = "enum";

    static String[] FLOAT_PREFIXES = {
        "float"
    };

    static String[] INTEGER_PREFIXES = {
        "int", "smallint", "tinyint"
    };

    static String[] LONG_PREFIXES = {
        "bigint"
    };

    static String[] STRING_PREFIXES = {
        "char", "varchar"
    };

    static String[] TEXT_PREFIXES = {
        "text"
    };

    static String[] DATE_PREFIXES = {
        "datetime"
    };

    public static Type fromDatabaseType(String databaseType) {
        for (String prefix : DATE_PREFIXES) {
            if (databaseType.startsWith(prefix)) return DATE;
        }

        for (String prefix : FLOAT_PREFIXES) {
            if (databaseType.startsWith(prefix)) return FLOAT;
        }

        for (String prefix : LONG_PREFIXES) {
            if (databaseType.startsWith(prefix)) return LONG;
        }

        for (String prefix : INTEGER_PREFIXES) {
            if (databaseType.startsWith(prefix)) return INTEGER;
        }

        if (databaseType.startsWith(ENUM_PREFIX)) {
            return ENUM;
        }

        for (String prefix : TEXT_PREFIXES) {
            if (databaseType.startsWith(prefix)) return TEXT;
        }

        for (String prefix : STRING_PREFIXES) {
            if (databaseType.startsWith(prefix)) return STRING;
        }

        return UNKNOWN;
    }
}
