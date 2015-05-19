package datamigration.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Utils {

    public static final Object ERROR = new Object();

	private static String TABLE_METADATA_QUERY = "SHOW COLUMNS  FROM %s";
	
	private static String RECORD_COUNT_QUERY = "SELECT COUNT(*) FROM %s";
	
	private static String QUERY = "SELECT * FROM %s LIMIT ? , ?";

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
		StringBuffer entityFieldName = new StringBuffer();
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

		StringBuffer sbf = new StringBuffer();
		for (String str : strings) {
			sbf.append(Utils.getColumnNameAsEntityField(str));
			sbf.append(",");
		}
		sbf.deleteCharAt(sbf.length() - 1);
		return sbf.toString();
	}

	public static String createEntitySequenceName(String entityName) {
		StringBuffer sbf = new StringBuffer();
		sbf.append(entityName);
		sbf.append("Sequence");
		return sbf.toString();
	}

	public static String createEntitySequenceId(String entityName) {
		StringBuffer sbf = new StringBuffer();
		sbf.append(entityName);
		sbf.append("Sequence");
		sbf.append("Id");
		return sbf.toString();
	}

	//
	public static void main(String args[]) {

		System.out.println(getMappingType("int(4) unsigned", "2"));
		System.out.println(getMappingType("varchar(255)", "Hello"));
		System.out.println(getMappingType("date", "2010-03-04"));
	}

	public static Object getMappingType(String databaseType, String value) {
		/*for (String dbType : DATABASE_TYPE_DATE) {
			if (databaseType.contains(dbType)) {
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				Date parsedDate = null;
				try {
					parsedDate = df.parse(value);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return parsedDate;
			}
		}*/

		return null;
	}

    public static Object map(String databaseType, String value) {
        Type type = Type.fromDatabaseType(databaseType);
        try {
            switch (type) {
                case FLOAT: return nullOrEmpty(value) ? null : Float.valueOf(value);
                case INTEGER: return nullOrEmpty(value) ? null : Integer.valueOf(value);
                case ENUM: return value;
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
}
