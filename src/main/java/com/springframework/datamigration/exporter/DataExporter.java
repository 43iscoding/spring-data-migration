package com.springframework.datamigration.exporter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.context.ApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 * @author Prasanth M P 
 */
public class DataExporter implements Runnable {

	private ApplicationContext context;

	private CountDownLatch countDownLatch;
	
	
	/**
	 *  The run() method create a fixed size thread pool of worker threads. 
	 *  The worker threads exports tables to CSV files
	 */
	public void run() {
		Properties configurationProperties = (Properties) context.getBean("threadPoolPropertiesConfiguration");
		ExecutorService executorService = Executors.newFixedThreadPool( Integer.valueOf(configurationProperties.getProperty("exportThreadPoolSize")));
		List<String> databaseTables = getDatabaseTableNames();
		CountDownLatch exporterCountLatch = new CountDownLatch(
				databaseTables.size());
		Collection<Future<?>> futures = new LinkedList<Future<?>>();
		for (String tableName : databaseTables) {
			TableExporter tableExporterBean = (TableExporter) context.getBean("tableExporter");
			tableExporterBean.setTableName(tableName.toUpperCase());
			tableExporterBean.setCountDownLatch(exporterCountLatch);
			futures.add(executorService.submit(tableExporterBean));
		}
		executorService.shutdown();
		try {
			for (Future<?> future:futures) {
				future.get(); // cause the current thread to wait for the table export tasks to finish.
			}
			exporterCountLatch.await();
			countDownLatch.countDown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * The method returns a list containing the names of all tables in the database.
	 * @return List<String>
	 */
	private List<String> getDatabaseTableNames() {
		final List<String> tablenames = new ArrayList<String>();
		JdbcTemplate jdbcTemplate = (JdbcTemplate) context
				.getBean("jdbcTemplate");
		String showTables = "SHOW TABLES";
		jdbcTemplate.query(showTables, new ResultSetExtractor<List<String>>() {
			public List<String> extractData(ResultSet rs) throws SQLException,
					DataAccessException {
				while (rs.next()) {
					tablenames.add(rs.getString(1));
				}
				return tablenames;
			}
		});
		
		tablenames.remove("data_export_result");
		tablenames.remove("data_import_result");
		return tablenames;
	}


	/**
	 * The getter method. 
	 */
	public ApplicationContext getContext() {
		return context;
	}
	
	/**
	 * The setter method. 
	 */
	public void setContext(ApplicationContext context) {
		this.context = context;
	}

	/**
	 * The getter method. 
	 */
	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}
	
	/**
	 * The setter method. 
	 */
	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}		
}
