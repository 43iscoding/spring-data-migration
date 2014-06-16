package com.springframework.datamigration;

import java.util.concurrent.CountDownLatch;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.springframework.datamigration.exporter.DataExporter;

public class SpringPetClinicDataMigration {

	public static ApplicationContext ctx;
	
	public static void main (String args[]) {
	 
	 loadApplicationContext();
	
	 
	 
	 CountDownLatch latch = new CountDownLatch(1);
	 
	 
	 DataExporter dataExporterThread = new DataExporter();
	 dataExporterThread.setContext(ctx);
	 
	 
	 new Thread(dataExporterThread).start();
	 
	
	 
	 
	
	}
	
	
	public static void loadApplicationContext(){
		 ctx = new ClassPathXmlApplicationContext("SpringDatabaseMigration.xml");
		 
		 
		 
	}
	
	
	
	
}
