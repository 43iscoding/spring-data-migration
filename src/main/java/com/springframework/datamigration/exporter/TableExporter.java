package com.springframework.datamigration.exporter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import com.springframework.datamigration.utils.Constants;

public class TableExporter implements Runnable {

	
	private CountDownLatch countDownLatch;
	
	private JdbcTemplate jdbcTemplate;
	
	@Value("${fetchSize}")
	protected int fetchSize;

	@Value("${migrationfolder}")
	protected String migrationFolder;
	
	private String folderName;

	private String fileNamePrefix;
	
	private String tableMetaData;
	
	private String tableMetaDataQuery;

	protected String recordCountQuery;

	private int recordCount;

	private String tableName;
	
 

	private String query;


	public void run() {
		
		System.out.println("********************");
	 		// getRecordCount();
	 		populateTableRecordCount();
	 		populateTableMetaData();
	 		try{
	 			exportToCSV();
	 		} catch(Exception e){
	 			updateExecutionStatus();
	 		}
	 		countDownLatch.countDown();
			System.out.println("********************");
	}
	
	


	


	public void populateTableRecordCount() {
		
		int recordCount =	getJdbcTemplate().queryForInt(getRecordCountQuery());
		setRecordCount(recordCount);
		
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
		return Constants.getTableMetaDataQuery(this.tableName);
	}

	public String getTableMetaData() {
		return tableMetaData;
	}
	
	public void setTableMetaData(String tableMetaData) {
		this.tableMetaData = tableMetaData;
	}
	

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
    public String getRecordCountQuery() {
		return Constants.getTableRecordCountQuery(this.tableName);
	}




	public int getRecordCount() {
		return recordCount;
	}
	
	
	public void setRecordCount(int recordCount){
		this.recordCount = recordCount;
	}
	
	
	public String getMigrationFolder() {
		return migrationFolder;
	}


	public void setMigrationFolder(String migrationFolder) {
		this.migrationFolder = migrationFolder;
	}
	
	   public String getQuery() {
			return Constants.getTableRecordSelectQuery(tableName);
		}


	
	
	public int csvFilesPerTable(){
		int noCSVFiles = 0;
		if( getRecordCount() < getFetchSize()){
			noCSVFiles = 1;
		} else if(getRecordCount() % getFetchSize() == 0 ) {
			noCSVFiles = getRecordCount() / getFetchSize() ;
		} else {
			noCSVFiles = getRecordCount() / getFetchSize() + 1;
		}
		return noCSVFiles;
	}
	
	
	public void prepareDirectory(){
		File dir = new File(this.migrationFolder+ "\\"+ getFolderName());
		if(dir.exists()){
			try {
				FileUtils.deleteDirectory(dir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		dir.mkdir();
	}
	
	

	public void populateTableMetaData() {
		String jdbcTableMetaDataQuery = getTableMetaDataQuery();
		final List<String> fields = new ArrayList<String>();
		
		getJdbcTemplate().query(jdbcTableMetaDataQuery, new RowMapper<String>() {
			
			public String mapRow(ResultSet rs, int rowNum) throws SQLException {
				fields.add(rs.getString(1));
				return null;
			}
		});

		setTableMetaData( fields.toString().replace("[", "").replace("]", ""));
	}
	
	
	   public String getFileContentToWrite(int lowerLimit){
		   
		   return getJdbcTemplate().query(  getQuery(), new Object[]{lowerLimit, fetchSize}, new ResultSetExtractor<String>() {
				
				public String extractData(ResultSet rs) throws SQLException,
						DataAccessException {
							StringBuffer fileContentsToWrite = null;
							fileContentsToWrite = new StringBuffer();
							fileContentsToWrite.append(getTableMetaData());
							fileContentsToWrite.append("\n");
							List<String> row = null;
							while(rs.next()){
								row = new ArrayList<String>();
								for(int columnNo=1; columnNo <= rs.getMetaData().getColumnCount(); columnNo++ ){
									row.add(rs.getString(columnNo));
								}
								fileContentsToWrite.append( row.toString().replace("[", "").replace("]","").concat("\n"));
								
							}
					return fileContentsToWrite.toString();
				}	
				
			});
	   }
	   

		public void exportToCSV() {
			int noCSVFiles = csvFilesPerTable();
			prepareDirectory();
			int lowerLimit = 0;
			
			for(int i=0;i<noCSVFiles;i++){
				
				File file = new File(this.migrationFolder+ "\\"+ getFolderName(), getFileNamePrefix()+i+".csv");
				PrintWriter  pw = null;
				try {
					file.createNewFile();
					pw = new PrintWriter(file);
				} catch (IOException e2) {
					e2.printStackTrace();
				}
				
				String fileContentsToWrite =	getFileContentToWrite(lowerLimit);
				pw.write(fileContentsToWrite);
				pw.flush();
				pw.close();
				lowerLimit = lowerLimit+ fetchSize;
			}
	   
		}
			
			public void updateExecutionStatus() {
				
			}
	
			
			
			public String getTableName() {
				return tableName;
			}


			public void setTableName(String tableName) {
				this.tableName = tableName;
			}

	
}
