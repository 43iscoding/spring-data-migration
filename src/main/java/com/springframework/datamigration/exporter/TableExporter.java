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

public class TableExporter implements Runnable {

	private CountDownLatch countDownLatch;

	private JdbcTemplate jdbcTemplate;

	@Value("${fetchSize}")
	protected int fetchSize;

	@Value("${migrationfolder}")
	protected String migrationFolder;

	private String tableColumnNames;

	private String tableColumnDatabaseTypes;

	protected String recordCountQuery;

	private int recordCount;

	private String tableName;

	public void run() {
		System.out.println("Starting to export Data from Table [ "
				+ getTableName() + " ] to CSV files");
		// getRecordCount();
		populateTableRecordCount();
		populateTableMetaData();
		try {
			exportToCSV();
			updateExecutionStatus(tableName,recordCount,Status.SUCCESS,new Date());
		} catch (Exception e) {
			updateExecutionStatus(tableName,null,Status.FAILURE,new Date());
		}
		countDownLatch.countDown();
		System.out.println("Finished exporting Data from Table [ "
				+ getTableName() + " ] to CSV files");
	}

	private void updateExecutionStatus( String tableName,Integer recordCount,
			Status status, Date date) {
		final String INSERT_SQL = "INSERT INTO DATA_EXPORT_RESULT (TABLE_NAME,"
				+ "ROWS_EXPORTED_COUNT,"
				+ "ROWS_EXPORT_STATUS,"
				+ "ROWS_EXPORTATION_DATE) VALUES (?,?,?,?)";
		jdbcTemplate.update(INSERT_SQL, tableName,recordCount,status.name(),new java.sql.Date(date.getTime()));
	}

	private void populateTableRecordCount() {
		int recordCount = getJdbcTemplate().queryForInt(getRecordCountQuery());
		setRecordCount(recordCount);
	}

	public String getQuery() {
		return Utils.getTableRecordSelectQuery(tableName);
	}

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
	
	
	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}

	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	public String getFolderName() {
		return tableName.toUpperCase();
	}

	public String getFileNamePrefix() {
		return tableName.toUpperCase();
	}

	public String getTableMetaDataQuery() {
		return Utils.getTableMetaDataQuery(this.tableName);
	}

	public String getTableColumnNames() {
		return tableColumnNames;
	}

	public void setTableColumnNames(String tableColumnNames) {
		this.tableColumnNames = tableColumnNames;
	}

	public String getTableColumnDatabaseTypes() {
		return tableColumnDatabaseTypes;
	}

	public void setTableColumnDatabaseTypes(String tableColumnDatabaseTypes) {
		this.tableColumnDatabaseTypes = tableColumnDatabaseTypes;
	}

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public String getRecordCountQuery() {
		return Utils.getTableRecordCountQuery(this.tableName);
	}

	public int getRecordCount() {
		return recordCount;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

	public String getMigrationFolder() {
		return migrationFolder;
	}

	public void setMigrationFolder(String migrationFolder) {
		this.migrationFolder = migrationFolder;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
