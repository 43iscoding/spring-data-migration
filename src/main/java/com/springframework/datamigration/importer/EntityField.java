package com.springframework.datamigration.importer;

public class EntityField {

	private String databaseColumnName;

	private String databaseColumnType;

	private String databaseColumnValue;

	public EntityField(String databaseColumnName, String databaseColumnType,
			String databaseColumnValue) {
		this.databaseColumnName = databaseColumnName;
		this.databaseColumnType = databaseColumnType;
		this.databaseColumnValue = databaseColumnValue;
	}

	public String getDatabaseColumnName() {
		return databaseColumnName;
	}

	public String getDatabaseColumnType() {
		return databaseColumnType;
	}

	public String getDatabaseColumnValue() {
		return databaseColumnValue;
	}

}
