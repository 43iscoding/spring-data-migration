package datamigration.importer;

import org.springframework.context.ApplicationContext;

import java.util.*;
import java.util.concurrent.*;

public class DataImporter implements Runnable {

	private ApplicationContext context;

	private CountDownLatch countDownLatch;


	/**
	 * The run() method create a fixed size thread pool of worker threads. The
	 * worker threads executes the tasks which import CSV files to GAE Entities.
	 */
	public void run() {

		Properties configurationProperties = (Properties) context
				.getBean("threadPoolPropertiesConfiguration");
		ExecutorService executorService = Executors.newFixedThreadPool(Integer
				.valueOf(configurationProperties
						.getProperty("importThreadPoolSize")));

		Map<String, String> tableToEntityMap = (Map<String, String>) context.getBean("tableToEntityMapping");
		Set<String> databaseTableNames = tableToEntityMap.keySet();
		CountDownLatch importerCountLatch = new CountDownLatch(databaseTableNames.size() - 1);

		Collection<Future<?>> futures = new LinkedList<Future<?>>();
		for (String tableName : databaseTableNames) {
			TableImporter tableImporter = context.getBean(TableImporter.class);
			tableImporter.setImporterCountLatch(importerCountLatch);
			tableImporter.setTableToExport(tableName);
			tableImporter.setEntityName(tableToEntityMap.get(tableName));
			tableImporter.setFolderName(tableName.toUpperCase());
			futures.add(executorService.submit(tableImporter));
		}
		executorService.shutdown();
		try {
			for (Future<?> future : futures) {
				future.get(); // cause the current thread to wait for the table import tasks to finish.
			}
			importerCountLatch.await();
			countDownLatch.countDown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	// getter method
	public ApplicationContext getContext() {
		return context;
	}
	
	//setter method
	public void setContext(ApplicationContext context) {
		this.context = context;
	}

	// getter method
	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}
	
	//setter method
	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}

}
