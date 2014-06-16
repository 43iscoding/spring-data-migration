package com.springframework.datamigration.utils;

public class Constants {
	
	private Constants(){}
	
	private static String TABLE_METADATA_QUERY="SHOW COLUMNS  FROM %s";
	private static String RECORD_COUNT_QUERY="SELECT COUNT(*) FROM %s";
	private static String QUERY="SELECT * FROM %s LIMIT ? , ?";
	
   public static String getTableMetaDataQuery(String tableName){
	   return  String.format(TABLE_METADATA_QUERY, tableName);
   }
   
   public static String getTableRecordCountQuery(String tableName){
	   return  String.format(RECORD_COUNT_QUERY, tableName);
   }
	
   public static String getTableRecordSelectQuery(String tableName){
	   return  String.format(QUERY, tableName);
   }
   

}
