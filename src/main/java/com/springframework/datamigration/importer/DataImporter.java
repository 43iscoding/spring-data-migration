package com.springframework.datamigration.importer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.ApplicationContext;

import com.springframework.datamigration.exporter.TableExporter;

public class DataImporter implements Runnable {




	private CountDownLatch countDownLatch;
	
	private ApplicationContext context;
	
	public void run() {
	


		
		 Map<String, TableImporter> tableImporterMap =	 context.getBeansOfType(TableImporter.class);
		 CountDownLatch importerCountLatch = new CountDownLatch(tableImporterMap.size());
		 ExecutorService executorService = Executors.newFixedThreadPool( 1)	;
		 Collection<TableImporter> tableImporterList =  tableImporterMap.values();
		 for(TableImporter tableImporter:tableImporterList ){
			 tableImporter.setImporterCountLatch(importerCountLatch);
			 executorService.submit(tableImporter);			 	 
		 }
		 
	
			 try {
				importerCountLatch.await();
				countDownLatch.countDown();
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

	
	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}


	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}

}
