package com.springframework.datamigration.exporter;

public class OwnersTableExporter extends TableExporter {

	@Override
	public void updateExecutionStatus() {
		
	}

	@Override
	public void exportToCSV() {
		
	}

	
	
	
	@Override
	public void getTableMetaData() {
		setTableMetaData("TEST");
	}

	@Override
	public void getRecordCount() {
	 int recordCount =	getJdbcTemplate().queryForInt(getQuery());
	 setRecordCount(recordCount);
	}

}
