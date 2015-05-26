package datamigration.exporter;

import datamigration.Migration;
import datamigration.utils.MapperConfig;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

public class DataExporter implements Runnable {

	private CountDownLatch countDownLatch;
	
	/**
	 *  The run() method create a fixed size thread pool of worker threads. 
	 *  The worker threads exports tables to CSV files
	 */
	public void run() {
		ExecutorService executorService = Executors.newFixedThreadPool(Migration.getIntProperty("exportThreadPoolSize"));
		List<String> databaseTables = getDatabaseTableNames();
        CountDownLatch exporterCountLatch = new CountDownLatch(
				databaseTables.size());
		Collection<Future<?>> futures = new LinkedList<Future<?>>();
		for (String tableName : databaseTables) {
			TableExporter tableExporterBean = Migration.getBean(TableExporter.class);
			tableExporterBean.setTableName(tableName.toUpperCase());
			tableExporterBean.setCountDownLatch(exporterCountLatch);
			futures.add(executorService.submit(tableExporterBean));
		}
		executorService.shutdown();
		try {
			for (Future<?> future:futures) {
				future.get();
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
        final MapperConfig config = Migration.getBean(MapperConfig.class);
		JdbcTemplate jdbcTemplate = Migration.getBean(JdbcTemplate.class);
		String showTables = "SHOW TABLES";
        System.out.println("Processing tables: ");
        jdbcTemplate.query(showTables, new ResultSetExtractor<List<String>>() {
			public List<String> extractData(ResultSet rs) throws SQLException,
					DataAccessException {
				while (rs.next()) {
                    String tableName = rs.getString(1);
                    if (config.shouldProcess(tableName)) {
                        System.out.println("* " + tableName.toUpperCase());
                        tablenames.add(tableName);
                    }
				}
				return tablenames;
			}
		});
        System.out.println("Total (" + tablenames.size() + ")");
        return tablenames;
	}

	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}		
}
