package com.springframework.datamigration.exporter;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.ApplicationContext;

public class DataExporter implements Runnable {


	private ApplicationContext context;
	
	public void run() {
	
	ExecutorService executorServer = Executors.newFixedThreadPool( 2)	;
		
	 Map<String, TableExporter> tableExporterMap =	 context.getBeansOfType(TableExporter.class);
	 Collection<TableExporter>  tableExporterThreads = tableExporterMap.values();
	 
	 CountDownLatch exporterCountLatch = new CountDownLatch(tableExporterThreads.size());
	 
	 
	 for(TableExporter tableExporter :tableExporterThreads){
		 tableExporter.setCountDownLatch(exporterCountLatch);
		 executorServer.submit(tableExporter);
	 }
	 executorServer.shutdown();
	
	 try {
		exporterCountLatch.await();
	} catch (InterruptedException e) {
		e.printStackTrace();
	}
	 
	}
	
	
	public ApplicationContext getContext() {
		return context;
	}

	public void setContext(ApplicationContext context) {
		this.context = context;
	}

	

}
