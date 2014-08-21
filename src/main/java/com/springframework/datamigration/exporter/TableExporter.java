package com.springframework.datamigration.exporter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import com.springframework.datamigration.utils.Status;
import com.springframework.datamigration.utils.Utils;

/**
 * @author Prasanth M P 
 */
public class TableExporter implements Runnable {

	private CountDownLatch countDownLatch;

	@Value("${fetchSize}")
	protected int fetchSize;

	private JdbcTemplate jdbcTemplate;

	@Value("${migrationfolder}")
	protected String migrationFolder;

	private int recordCount;

	private String tableColumnDatabaseTypes;

	private String tableColumnNames;

	private String tableName;
	
	/**
	 * The run() method contains the workflow logic for exporting the records in
	 * the table to CSV files and also for logging the result of export to the
	 * database.
	 */
	public void run() {

		System.out.println("Starting to export Data from Table [ "
				+ getTableName() + " ] to CSV files");
		// getRecordCount();
		populateTableRecordCount();
		populateTableMetaData();
		try {
			exportToCSV();
			updateExecutionStatus(tableName, recordCount, Status.SUCCESS,
					new Date());
		} catch (Exception e) {
			updateExecutionStatus(tableName, null, Status.FAILURE, new Date());
		}
		countDownLatch.countDown();
		System.out.println("Finished exporting Data from Table [ "
				+ getTableName() + " ] to CSV files");
	}
	
	/**
	 * The method gets the count of total number of records in the table.
	 */
	private void populateTableRecordCount() {
		int recordCount = getJdbcTemplate().queryForInt(getRecordCountQuery());
		setRecordCount(recordCount);
	}
	
	/**
	 * The method generates the meta data for inserting in each CSV export file.
	 * The meta data involves the colum names and column types.
	 */
	private void populateTableMetaData() {
		String jdbcTableMetaDataQuery = getTableMetaDataQuery();
		final List<String> columnName = new ArrayList<String>();
		final List<String> columnType = new ArrayList<String>();
		getJdbcTemplate().query(jdbcTableMetaDataQuery,
				new RowMapper<String>() {
					public String mapRow(ResultSet rs, int rowNum)
							throws SQLException {
						columnName.add(rs.getString(1));
						columnType.add(rs.getString(2));
						return null;
					}
				});

		setTableColumnNames(Utils.getCSV(columnName));
		setTableColumnDatabaseTypes(Utils.getCSV(columnType));
	}
	

	/**
	 * The method creates CSV files and export the records in the table in
	 * batches.
	 */
	private void exportToCSV() {
		int noCSVFiles = csvFilesPerTable();
		prepareDirectory();
		int lowerLimit = 0;
		PrintWriter pw = null;
		for (int i = 0; i < noCSVFiles; i++) {
			File file = new File(this.migrationFolder + "\\" + getFolderName(),
					getFileNamePrefix() + i + ".csv");
			try {
				file.createNewFile();
				pw = new PrintWriter(file);
			} catch (IOException e2) {
				e2.printStackTrace();
			}
			String fileContentsToWrite = getFileContentToWrite(lowerLimit);
			pw.write(fileContentsToWrite);
			pw.flush();
			pw.close();
			lowerLimit = lowerLimit + fetchSize;
		}
	}
	
	/**
	 * Indicates how many CSV files will be created for storing the records in
	 * the table. The number of CSV files depends on the fetch size configure.
	 * More the fetch size lesser the number of CSV files.
	 * 
	 * @return integer
	 */
	public int csvFilesPerTable() {
		int noCSVFiles = 0;
		if (getRecordCount() < getFetchSize()) {
			noCSVFiles = 1;
		} else if (getRecordCount() % getFetchSize() == 0) {
			noCSVFiles = getRecordCount() / getFetchSize();
		} else {
			noCSVFiles = getRecordCount() / getFetchSize() + 1;
		}
		return noCSVFiles;
	}

	/**
	 * The method just update the status of the export of the table.
	 * 
	 * @param tableName - The name of the table in the database exported as CSV file
	 * @param recordCount - The number of records that are exported.
	 * @param status - The status of the export of the table
	 * @param date - The date on which the table exported.
	 */
	private void updateExecutionStatus(String tableName, Integer recordCount,
			Status status, Date date) {
		final String INSERT_SQL = "INSERT INTO DATA_EXPORT_RESULT (TABLE_NAME,"
				+ "ROWS_EXPORTED_COUNT," + "ROWS_EXPORT_STATUS,"
				+ "ROWS_EXPORTATION_DATE) VALUES (?,?,?,?)";
		jdbcTemplate.update(INSERT_SQL, tableName, recordCount, status.name(),
				new java.sql.Date(date.getTime()));
	}
	
	/**
	 * The method fetches a batch of records and parse the records in CSV format
	 * to be returned.
	 * 
	 * @param lowerLimit - the lower limit used to calculate the range of records to be
	 *            fetched for exporting. range equal to (lowerLimit -->
	 *            lowerLimit+fetchSize)
	 * @return String - returns the fetched records in CSV format to be written
	 *         to a CSV file.
	 */
	public String getFileContentToWrite(int lowerLimit) {
		return getJdbcTemplate().query(getQuery(),
				new Object[] { lowerLimit, fetchSize },
				new ResultSetExtractor<String>() {
					public String extractData(ResultSet rs)
							throws SQLException, DataAccessException {
						StringBuffer fileContentsToWrite = null;
						fileContentsToWrite = new StringBuffer();
						fileContentsToWrite.append(getTableColumnNames());
						fileContentsToWrite.append("\n");
						fileContentsToWrite
								.append(getTableColumnDatabaseTypes());
						fileContentsToWrite.append("\n");
						List<String> row = null;
						while (rs.next()) {
							row = new ArrayList<String>();
							for (int columnNo = 1; columnNo <= rs.getMetaData()
									.getColumnCount(); columnNo++) {
								row.add(rs.getString(columnNo));
							}
							fileContentsToWrite.append(Utils.getCSV(row)
									.concat("\n"));
						}
						return fileContentsToWrite.toString();
					}
				});
	}
	
	/**
	 * Creates a directory for each table to be exported where the related CSV
	 * files will be placed.
	 */
	public void prepareDirectory() {
		File dir = new File(this.migrationFolder + "\\" + getFolderName());
		if (dir.exists()) {
			try {
				FileUtils.deleteDirectory(dir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		dir.mkdir();
	}		

	//getter method
	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}
	
	//setter method
	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}
	
	//getter method
	public int getFetchSize() {
		return fetchSize;
	}
	
	//setter method
	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	//getter method
	public String getFileNamePrefix() {
		return tableName.toUpperCase();
	}

	//getter method
	public String getFolderName() {
		return tableName.toUpperCase();
	}
	
	//getter method
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	//setter method
	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
	//getter method
	public String getMigrationFolder() {
		return migrationFolder;
	}
	
	//setter method
	public void setMigrationFolder(String migrationFolder) {
		this.migrationFolder = migrationFolder;
	}

	//getter method
	public String getQuery() {
		return Utils.getTableRecordSelectQuery(tableName);
	}

	//getter method
	public int getRecordCount() {
		return recordCount;
	}

	//setter method
	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

	//getter method
	public String getRecordCountQuery() {
		return Utils.getTableRecordCountQuery(this.tableName);
	}

	//getter method
	public String getTableColumnDatabaseTypes() {
		return tableColumnDatabaseTypes;
	}
	
	//setter method
	public void setTableColumnDatabaseTypes(String tableColumnDatabaseTypes) {
		this.tableColumnDatabaseTypes = tableColumnDatabaseTypes;
	}

	//getter method
	public String getTableColumnNames() {
		return tableColumnNames;
	}
	
	//setter method
	public void setTableColumnNames(String tableColumnNames) {
		this.tableColumnNames = tableColumnNames;
	}

	//getter method
	public String getTableMetaDataQuery() {
		return Utils.getTableMetaDataQuery(this.tableName);
	}

	//getter method
	public String getTableName() {
		return tableName;
	}

	//setter method
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
