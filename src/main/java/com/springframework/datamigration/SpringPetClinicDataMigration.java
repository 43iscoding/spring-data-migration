package com.springframework.datamigration;

import java.util.concurrent.CountDownLatch;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.springframework.datamigration.exporter.DataExporter;
import com.springframework.datamigration.importer.DataImporter;

public class SpringPetClinicDataMigration {

	public static ApplicationContext ctx;
	
	public static void main (String args[]) {
	 
	 loadApplicationContext();
	
	 
	 
	 CountDownLatch latch1 = new CountDownLatch(1);
	 
	 
	 DataExporter dataExporterThread = new DataExporter();
	 dataExporterThread.setContext(ctx);
	 dataExporterThread.setCountDownLatch(latch1);
	 Thread t1 = new Thread(dataExporterThread);
	 t1.start();

	 try {
		latch1.await();
		
	} catch (InterruptedException e) {
		
		e.printStackTrace();
	}
	 
	 
	 CountDownLatch latch2 = new CountDownLatch(1);
	 
	 System.out.println("Now running importer");
	 DataImporter dataImporterThread = new DataImporter();
	 dataImporterThread.setContext(ctx);
	 dataImporterThread.setCountDownLatch(latch2);
	 Thread t2 = new Thread(dataImporterThread);
	 t2.start();
	 
	
	 try {
			latch2.await();
			
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
		 
	 
	 
	
	}
	
	
	public static void loadApplicationContext(){
		 ctx = new ClassPathXmlApplicationContext("SpringDatabaseMigration.xml");
		 
		 
		 
	}
	
	
	
	
}
