package datamigration.importer;

import datamigration.Migration;
import datamigration.utils.Mapper;
import datamigration.utils.MapperConfig;

import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.*;

public class DataImporter implements Runnable {

	private CountDownLatch countDownLatch;

	/**
	 * The run() method create a fixed size thread pool of worker threads. The
	 * worker threads executes the tasks which import CSV files to GAE Entities.
	 */
	public void run() {
		ExecutorService executorService = Executors.newFixedThreadPool(Migration.getIntProperty("importThreadPoolSize"));

        MapperConfig config = Migration.getBean(MapperConfig.class);
        CountDownLatch importerCountLatch = config.getCountDownLatch();

        Collection<Future<?>> futures = new LinkedList<Future<?>>();

        for (Mapper mapper : config.getMappers()) {
            TableImporter tableImporter = Migration.getBean(TableImporter.class);
            tableImporter.setImporterCountLatch(importerCountLatch);
            tableImporter.setTableToExport(mapper.getTableName());
            tableImporter.setEntityName(mapper.getEntityName());
            tableImporter.setFolderName(mapper.getFolderName());
            tableImporter.setFieldsToIgnore(mapper.getFieldsToIgnore());
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

	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}
}
