package com.springframework.datamigration.exporter;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

public abstract class TableExporter implements Runnable {

	
	private JdbcTemplate jdbcTemplate;
	
	private int fetchSize;

	private String folderName;

	private String fileNamePrefix;
	
	private String tableMetaData;

	protected String query;

    private int recordCount;
	


	public void run() {
	 		getRecordCount();
	 		getTableMetaData();
	 		try{
	 			exportToCSV();
	 		} catch(Exception e){
	 			updateExecutionStatus();
	 		}
	}
	
	
	public abstract void updateExecutionStatus();

	public abstract void exportToCSV();

	public abstract void getTableMetaData();

	public abstract void getRecordCount();

	public int getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

	public String getFolderName() {
		return folderName;
	}

	public void setFolderName(String folderName) {
		this.folderName = folderName;
	}

	public String getFileNamePrefix() {
		return fileNamePrefix;
	}

	public void setFileNamePrefix(String fileNamePrefix) {
		this.fileNamePrefix = fileNamePrefix;
	}

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
    public String getQuery() {
		return query;
	}


	public void setQuery(String query) {
		this.query = query;
	}

	public void setTableMetaData(String tableMetaData) {
		this.tableMetaData = tableMetaData;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}
	
}
