package com.springframework.datamigration.importer;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.ApplicationContext;

public class DataImporter implements Runnable {

	private CountDownLatch countDownLatch;

	private ApplicationContext context;

	public void run() {

		// Map<String, TableImporter> tableImporterMap =
		// context.getBeansOfType(TableImporter.class);

		Map<String, String> tableToEntityMap = (Map<String, String>) context
				.getBean("tableToEntityMapping");

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		Set<String> databaseTableNames = tableToEntityMap.keySet();
		CountDownLatch importerCountLatch = new CountDownLatch(
				databaseTableNames.size() - 1);

		for (String tableName : databaseTableNames) {
			TableImporter tableImporter = context.getBean(TableImporter.class);
			tableImporter.setImporterCountLatch(importerCountLatch);
			tableImporter.setTableToExport(tableName);
			tableImporter.setEntityName(tableToEntityMap.get(tableName));
			tableImporter.setFolderName(tableName.toUpperCase());
			executorService.submit(tableImporter);
		}
		executorService.shutdown();

		// Collection<TableImporter> tableImporterList =
		// tableImporterMap.values();
		//
		// for(TableImporter tableImporter:tableImporterList ){
		// tableImporter.setImporterCountLatch(importerCountLatch);
		// executorService.submit(tableImporter);
		// }

		try {
			importerCountLatch.await();
			countDownLatch.countDown();
		} catch (InterruptedException e) {

			e.printStackTrace();
		} finally {

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
