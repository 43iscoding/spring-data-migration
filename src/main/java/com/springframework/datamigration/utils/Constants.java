package com.springframework.datamigration.utils;

import java.util.ArrayList;
import java.util.List;

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
   
//   
//   public static void main(String args[]){
//	   List<String> str = new ArrayList<String>();
//	   str.add("abc1");
//	   str.add("abc2");
//	   str.add("abc3");
//	   str.add("abc4");
//	   System.out.println(getCSV(str));
//   }
   
   public static String getCSV(List<String> strings){
	   
	   StringBuffer sbf = new StringBuffer();
	   for(String str:strings ){
		   sbf.append(str);
		   sbf.append(",");
	   }
	   sbf.deleteCharAt(sbf.length()-1);
	   return sbf.toString();
   }
   

}
