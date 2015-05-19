package datamigration.utils;

public enum Type {
    INTEGER,
    FLOAT,
    STRING,
    ENUM,
    UNKNOWN;

    static String ENUM_PREFIX = "enum";

    static String[] FLOAT_PREFIXES = {
            "float"
    };

    static String[] INTEGER_PREFIXES = {
            "int", "smallint"
    };

    static String[] STRING_PREFIXES = {
        "char", "varchar"
    };

    public static Type fromDatabaseType(String databaseType) {
        for (String prefix : FLOAT_PREFIXES) {
            if (databaseType.startsWith(prefix)) return FLOAT;
        }

        for (String prefix : INTEGER_PREFIXES) {
            if (databaseType.startsWith(prefix)) return INTEGER;
        }

        if (databaseType.startsWith(ENUM_PREFIX)) {
            return ENUM;
        }

        for (String prefix : STRING_PREFIXES) {
            if (databaseType.startsWith(prefix)) return STRING;
        }

        return UNKNOWN;
    }
}
