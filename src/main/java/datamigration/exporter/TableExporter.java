package datamigration.exporter;

import datamigration.utils.Status;
import datamigration.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class TableExporter implements Runnable {

    private String migrationFolder;
	private int fetchSize;
    private int limit; //We can limit total amount of entries per table for testing purposes

	private JdbcTemplate jdbcTemplate;
    private CountDownLatch countDownLatch;
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
		System.out.println("Export started from Table [ " + tableName + " ]");
        long startTime = Utils.getTime();
		populateTableRecordCount();
		populateTableMetaData();
		try {
			exportToCSV();
			updateExecutionStatus(tableName, recordCount, Status.SUCCESS, startTime);
		} catch (Exception e) {
            e.printStackTrace();
            updateExecutionStatus(tableName, null, Status.FAILURE, startTime);
		}
		countDownLatch.countDown();
	}
	
	/**
	 * The method gets the count of total number of records in the table.
	 */
	private void populateTableRecordCount() {
		Integer recordCount = jdbcTemplate.queryForObject(getRecordCountQuery(), Integer.class);
		setRecordCount(recordCount != null ? recordCount : 0);
	}
	
	/**
	 * The method generates the meta data for inserting in each CSV export file.
	 * The meta data involves the colum names and column types.
	 */
	private void populateTableMetaData() {
		final List<String> columnName = new ArrayList<String>();
		final List<String> columnType = new ArrayList<String>();
        jdbcTemplate.query(getTableMetaDataQuery(),
                new RowMapper<String>() {
                    public String mapRow(ResultSet rs, int rowNum)
                            throws SQLException {
                        columnName.add(normalize(rs.getString(1)));
                        columnType.add(normalize(rs.getString(2)));
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
        System.out.println("Creating " + noCSVFiles + " CSV files for [ " + tableName + " ]");
        for (int i = 0; i < noCSVFiles; i++) {
			File file = new File(this.migrationFolder + "\\" + tableName,
					tableName + i + ".csv");
			try {
                System.out.println("Processing export for [ " + tableName + " ] - " + (i + 1) + "/" + noCSVFiles + " CSV files");
				if (file.createNewFile()) {
                    PrintWriter pw = new PrintWriter(file);
                    String fileContentsToWrite = getFileContentToWrite(lowerLimit);
                    pw.write(fileContentsToWrite);
                    pw.flush();
                    pw.close();
                    lowerLimit = lowerLimit + fetchSize;
                } else {
                    System.out.println("Could not create file: " + file.getAbsolutePath());
                }
			} catch (IOException e2) {
				e2.printStackTrace();
			}
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
		int noCSVFiles;

        if (limit > 0) {
            recordCount = Math.min(recordCount, limit);
        }

		if (recordCount < fetchSize) {
			noCSVFiles = 1;
		} else if (recordCount % fetchSize == 0) {
			noCSVFiles = recordCount / fetchSize;
		} else {
			noCSVFiles = recordCount / fetchSize + 1;
		}

		return noCSVFiles;
	}

	/**
	 * The method just update the status of the export of the table.
	 * 
	 * @param tableName - The name of the table in the database exported as CSV file
	 * @param count - The number of records that are exported.
	 * @param status - The status of the export of the table
     * @param time - Start time of import
	 */
	private void updateExecutionStatus(final String tableName, final Integer count,
			final Status status, final long time) {
        System.out.println("Import finished for [ " + tableName +
                " ] (" + count + " entries). STATUS = " + status + " (" + (Utils.getTime() - time) + "ms)");
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
        int fetch = limit > 0 ? Math.min(limit - lowerLimit, fetchSize) : fetchSize;
		return jdbcTemplate.query(getQuery(),
				new Object[] { lowerLimit, fetch },
				new ResultSetExtractor<String>() {
					public String extractData(ResultSet rs)
							throws SQLException, DataAccessException {
						StringBuffer fileContentsToWrite;
						fileContentsToWrite = new StringBuffer();
						fileContentsToWrite.append(tableColumnNames);
						fileContentsToWrite.append("\n");
						fileContentsToWrite
								.append(tableColumnDatabaseTypes);
						fileContentsToWrite.append("\n");
						List<String> row;
						while (rs.next()) {
							row = new ArrayList<String>();
							for (int columnNo = 1; columnNo <= rs.getMetaData().getColumnCount(); columnNo++) {
								row.add(normalize(rs.getString(columnNo)));
                            }
							fileContentsToWrite.append(Utils.getCSV(row)
									.concat("\n"));
						}
						return fileContentsToWrite.toString();
					}
				});
	}

    /**
     * Replaces comma with semicolon
     */
    private String normalize(String input) {
        if (input == null) return null;

        return input.replace(',', ';');
    }
	
	/**
	 * Creates a directory for each table to be exported where the related CSV
	 * files will be placed.
	 */
	public void prepareDirectory() {
		File dir = new File(this.migrationFolder + "\\" + tableName);
		if (dir.exists()) {
			try {
				FileUtils.deleteDirectory(dir);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (!dir.mkdir()) {
            System.out.println("Could not create folder: " + dir);
        }
	}		

	public void setCountDownLatch(CountDownLatch countDownLatch) {
		this.countDownLatch = countDownLatch;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void setMigrationFolder(String migrationFolder) {
		this.migrationFolder = migrationFolder;
	}

	public String getQuery() {
		return Utils.getTableRecordSelectQuery(tableName);
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

	public String getRecordCountQuery() {
		return Utils.getTableRecordCountQuery(tableName);
	}

	public void setTableColumnDatabaseTypes(String tableColumnDatabaseTypes) {
		this.tableColumnDatabaseTypes = tableColumnDatabaseTypes;
	}

	public void setTableColumnNames(String tableColumnNames) {
		this.tableColumnNames = tableColumnNames;
	}

	public String getTableMetaDataQuery() {
		return Utils.getTableMetaDataQuery(tableName);
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
