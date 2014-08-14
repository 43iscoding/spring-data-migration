package com.springframework.datamigration;

import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import com.springframework.datamigration.exporter.DataExporter;
import com.springframework.datamigration.importer.DataImporter;

public class SpringDataMigration {

	public static ApplicationContext ctx;

	public static Map<String, String> map;

	public static void main(String args[]) {

		try {

			loadApplicationContext();
            init();
			System.out
					.println("***********************************************");
			System.out.println("Starting Data Migration");
			System.out.println("");

			Calendar cal1 = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			long time1 = cal1.getTimeInMillis();
			
			CountDownLatch latch1 = new CountDownLatch(1);
			DataExporter dataExporterThread = new DataExporter();
			dataExporterThread.setContext(ctx);
			dataExporterThread.setCountDownLatch(latch1);
			Thread t1 = new Thread(dataExporterThread);
			t1.start();

			latch1.await();
			CountDownLatch latch2 = new CountDownLatch(1);

			System.out.println("");
			System.out
					.println("Exporting Database Tables to CSV files is Completed and now Start to Export CSV files to GAE Entities");
			System.out.println("");

			DataImporter dataImporterThread = new DataImporter();
			dataImporterThread.setContext(ctx);
			dataImporterThread.setCountDownLatch(latch2);
			Thread t2 = new Thread(dataImporterThread);
			t2.start();
			latch2.await();

			Calendar cal2 = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
			long time2 = cal2.getTimeInMillis();
			
			System.out.println("Time in millisecond "+ (time2-time1));
			
			System.out.println("");
			System.out.println("Completed Data Migration");
			System.out
					.println("***********************************************");

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static void init() {
		JdbcTemplate jdbcTemplate = ctx.getBean("jdbcTemplate",JdbcTemplate.class);
		
		try{
			jdbcTemplate.execute("DROP TABLE DATA_EXPORT_RESULT");
		} catch(Exception e){}
		
		try{
			jdbcTemplate.execute("DROP TABLE DATA_IMPORT_RESULT");
		} catch(Exception e){}
		
		
		jdbcTemplate.execute("CREATE TABLE DATA_EXPORT_RESULT ("+
				"TABLE_NAME VARCHAR(50),"+
				"ROWS_EXPORTED_COUNT INTEGER,"+
				"ROWS_EXPORT_STATUS VARCHAR(100),"+
				"ROWS_EXPORTATION_DATE DATE)");
		
		jdbcTemplate.execute("CREATE TABLE DATA_IMPORT_RESULT ("+
				"ENTITY_NAME VARCHAR(50),"+
				"ENTITIES_CREATED_COUNT INTEGER,"+
				"ENTITIES_CREATION_STATUS VARCHAR(100),"+
				"ENTITIES_CREATION_DATE DATE)");
	} 

	public static void loadApplicationContext() {
		ctx = new ClassPathXmlApplicationContext("SpringDatabaseMigration.xml");
	}
	
	
	
	

}
