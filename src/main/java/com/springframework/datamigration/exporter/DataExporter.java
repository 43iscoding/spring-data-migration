package com.springframework.datamigration.exporter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.ApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class DataExporter implements Runnable {

	private ApplicationContext context;

	private CountDownLatch countDownLatch;

	public void run() {

		ExecutorService executorService = Executors.newFixedThreadPool(5);
		List<String> databaseTables = getDatabaseTableNames();

		CountDownLatch exporterCountLatch = new CountDownLatch(
				databaseTables.size());
		for (String tableName : databaseTables) {
			TableExporter tableExporterBean = (TableExporter) context
					.getBean("tableExporter");
			tableExporterBean.setTableName(tableName.toUpperCase());
			tableExporterBean.setCountDownLatch(exporterCountLatch);
			executorService.submit(tableExporterBean);
		}

		executorService.shutdown();

		try {
			exporterCountLatch.await();
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
		return tablenames;
	}

	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}

	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}

}
