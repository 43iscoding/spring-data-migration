package com.springframework.datamigration;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.springframework.datamigration.exporter.DataExporter;
import com.springframework.datamigration.importer.DataImporter;

public class SpringPetClinicDataMigration {

	public static ApplicationContext ctx;
	
	public static Map<String,String> map ;
	
	public static void main (String args[]) {
	
	try {
		
		
		
		
		 loadApplicationContext();
		 
		 System.out.println("***********************************************");
		 System.out.println("Starting Data Migration");
		 System.out.println("");
		 
		 CountDownLatch latch1 = new CountDownLatch(1);
		 DataExporter dataExporterThread = new DataExporter();
		 dataExporterThread.setContext(ctx);
		 dataExporterThread.setCountDownLatch(latch1);
		 Thread t1 = new Thread(dataExporterThread);
		 t1.start();
	
		 latch1.await();
		 CountDownLatch latch2 = new CountDownLatch(1);
		 
		 System.out.println("");
		 System.out.println("Exporting Database Tables to CSV files is Completed and now Start to Export CSV files to GAE Entities");
		 System.out.println("");
		 
		 DataImporter dataImporterThread = new DataImporter();
		 dataImporterThread.setContext(ctx);
		 dataImporterThread.setCountDownLatch(latch2);
		 Thread t2 = new Thread(dataImporterThread);
		 t2.start();
		 latch2.await();
		 
		 System.out.println("");
		 System.out.println("Completed Data Migration");
		 System.out.println("***********************************************");
		 
	} catch (InterruptedException e) {
			e.printStackTrace();
	} catch (Exception e){
			e.printStackTrace();
	}
	
	}
	
	
	public static void loadApplicationContext(){
		 ctx = new ClassPathXmlApplicationContext("SpringDatabaseMigration.xml");
	}
	
	
}
