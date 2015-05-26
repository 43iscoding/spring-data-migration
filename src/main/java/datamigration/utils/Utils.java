package datamigration.utils;

import com.google.appengine.api.datastore.Text;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Utils {

    public static final Object ERROR = new Object();

	private static final String TABLE_METADATA_QUERY = "SHOW COLUMNS  FROM %s";
	
	private static final String RECORD_COUNT_QUERY = "SELECT COUNT(*) FROM %s";
	
	private static final String QUERY = "SELECT * FROM %s LIMIT ? , ?";

	public static String getTableMetaDataQuery(String tableName) {
		return String.format(TABLE_METADATA_QUERY, tableName);
	}

	public static String getTableRecordCountQuery(String tableName) {
		return String.format(RECORD_COUNT_QUERY, tableName);
	}

	public static String getTableRecordSelectQuery(String tableName) {
		return String.format(QUERY, tableName);
	}

	public static String getColumnNameAsEntityField(String columnName) {
		
		if(columnName==null){
			return "";
		}
		
		List<String> str = new ArrayList<String>();
		if (columnName.contains("_")) {
			columnName = columnName.toLowerCase();
			StringTokenizer strTokenizer = new StringTokenizer(columnName, "_");
			while (strTokenizer.hasMoreTokens()) {
				str.add(strTokenizer.nextToken());
			}
		} else {
			str.add(columnName);
		}
        StringBuilder entityFieldName = new StringBuilder();
		for (int i = 0; i < str.size(); i++) {
			if (i != 0) {
				String a = str.get(i);
				char ch = Character.toUpperCase(a.charAt(0));
				entityFieldName.append(String.valueOf(ch)).append(
						str.get(i).substring(1));

			} else {
				entityFieldName.append(str.get(i));
			}
		}
		
		
		return entityFieldName.toString();
	}

	public static String getCSV(List<String> strings) {

        StringBuilder s = new StringBuilder();
		for (String str : strings) {
			s.append(Utils.getColumnNameAsEntityField(str));
			s.append(",");
		}
		s.deleteCharAt(s.length() - 1);
		return s.toString();
	}

	public static String createEntitySequenceName(String entityName) {
        StringBuilder s = new StringBuilder();
		s.append(entityName);
		s.append("Sequence");
		return s.toString();
	}

	public static String createEntitySequenceId(String entityName) {
        StringBuilder s = new StringBuilder();
		s.append(entityName);
		s.append("Sequence");
		s.append("Id");
		return s.toString();
	}

    public static Object map(String databaseType, String value) {
        Type type = Type.fromDatabaseType(databaseType);
        try {
            switch (type) {
                case DATE: {
                    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date parsedDate = null;
                    try {
                        parsedDate = df.parse(value);
                    } catch (ParseException e) {
                        if (!nullOrEmpty(value)) {
                            System.out.println("Could not parse date: " + e.getMessage());
                        }
                    }
                    return parsedDate;
                }
                case FLOAT: return nullOrEmpty(value) ? null : Float.valueOf(value);
                case LONG: return nullOrEmpty(value) ? null : Long.valueOf(value);
                case INTEGER: return nullOrEmpty(value) ? null : Integer.valueOf(value);
                case ENUM: return value;
                case TEXT: return new Text(value);
                case STRING: return value;
                default: //Unknown
                    System.out.println("Unknown type " + databaseType + " -> Fallback to string");
                    return value;
            }
        } catch (NumberFormatException e) {
            return ERROR;
        }
    }

    private static boolean nullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }

    public static long getTime() {
        return Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();
    }
}
