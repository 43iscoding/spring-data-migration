package com.springframework.datamigration.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;
import com.springframework.datamigration.utils.Status;
import com.springframework.datamigration.utils.Utils;

public class TableImporter implements Runnable {

	protected RemoteApiInstaller remoteApiInstaller;

	private RemoteApiOptions remoteApiOptions;
	
	private JdbcTemplate jdbcTemplate;

	private String folderName;

	@Value("${hostname}")
	private String hostname;

	@Value("${port}")
	private int port;

	@Value("${userEmail}")
	private String userEmail;

	@Value("${password}")
	private String password;
	
	private int entitiesExportCount;
	

	public String getTableToExport() {
		return tableToExport;
	}

	public void setTableToExport(String tableToExport) {
		this.tableToExport = tableToExport;
	}

	private String tableToExport;

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	private String entityName;

	private CountDownLatch importerCountLatch;

	@Value("${migrationfolder}")
	protected String migrationFolder;

	public String getMigrationFolder() {
		return migrationFolder;
	}

	public void setMigrationFolder(String migrationFolder) {
		this.migrationFolder = migrationFolder;
	}

	public RemoteApiInstaller getRemoteApiInstaller() {
		return remoteApiInstaller;
	}

	public void setRemoteApiInstaller(RemoteApiInstaller remoteApiInstaller) {
		this.remoteApiInstaller = remoteApiInstaller;
	}

	public RemoteApiOptions getRemoteApiOptions() {
		return remoteApiOptions;
	}

	public void setRemoteApiOptions(RemoteApiOptions remoteApiOptions) {
		this.remoteApiOptions = remoteApiOptions;
	}

	public String getFolderName() {
		return folderName;
	}

	public void setFolderName(String folderName) {
		this.folderName = folderName;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}
	
	public void run() {

		System.out
				.println("Creating App Engine Datastore Entities for table [ "
						+ tableToExport + " ] from the CSV files");
		List<File> files = getFiles();
		try {
			exportToAppEngineDataStore(files);
			updateExecutionStatus(entityName,entitiesExportCount,Status.SUCCESS,new Date());

			System.out
					.println("Completed Creation of App Engine Datastore Entities for table [ "
							+ tableToExport + " ] from the CSV files");
			
			Thread.sleep(1000);
			importerCountLatch.countDown();
			
		} catch(Exception e){
			e.printStackTrace();
			updateExecutionStatus(entityName,entitiesExportCount,Status.FAILURE,new Date());
		} finally {
		}

	}

	private void updateExecutionStatus(final String entityName,
			final Integer entitiesExportCount,final Status status,final Date date) {
		
		final String INSERT_SQL = "INSERT INTO DATA_IMPORT_RESULT ("
				+ "ENTITY_NAME,"
				+ "ENTITIES_CREATED_COUNT,"
				+ "ENTITIES_CREATION_STATUS,"
				+ "ENTITIES_CREATION_DATE) VALUES (?,?,?,?)";
		
		
		jdbcTemplate.update(INSERT_SQL, entityName,entitiesExportCount,status.name(),new java.sql.Date(date.getTime()));
	}

	public void exportToAppEngineDataStore(List<File> files) throws IOException {
		for (File file : files) {
			List<String> lines = readFile(file);
			List<Entity> entities = retrieveEntities(lines);
			entitiesExportCount = entitiesExportCount+ entities.size();
			saveToDataStore(entities);
		}
		createSequence(entityName, Utils.createEntitySequenceName(entityName),
				Utils.createEntitySequenceId(entityName));
	}



	public void saveToDataStore(List<Entity> entities) throws IOException {

		// initialization();

		remoteApiOptions.server(hostname, port)
				.credentials(userEmail, password);

		try {
			getRemoteApiInstaller().install(getRemoteApiOptions());
			DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
			ds.put(entities);
		} catch (IOException e) {
			throw e;
		} finally {
			getRemoteApiInstaller().uninstall();
		}

	

	}

	public List<Entity> retrieveEntities(List<String> lines) {

		List<Entity> entities = new ArrayList<Entity>();

		List<EntityUnit> entityMapList = new ArrayList<EntityUnit>();
		if (lines.size() > 2) {
			String[] columnNames = lines.get(0).split(",");
			String[] columnTypes = lines.get(1).split(",");

			for (int i = 2; i < lines.size(); i++) {

				String[] columnValuesInRow = lines.get(i).split(",");

				entityMapList.add(getEntityUnitToPersist(columnNames,
						columnTypes, columnValuesInRow));

			}
			createEntities(entities, entityMapList);
		}
		return entities;
	}

	private EntityUnit getEntityUnitToPersist(String[] columnNames,
			String[] columnTypes, String[] columnValuesInRow) {
		EntityUnit entityUnit = new EntityUnit();
		for (int i = 0; i < columnNames.length; i++) {
			entityUnit.addFieldToPersist(new EntityField(columnNames[i],
					columnTypes[i], columnValuesInRow[i]));
		}
		return entityUnit;
	}

	public void createEntities(List<Entity> entities,
			List<EntityUnit> entityMapList) {

		remoteApiOptions.server(hostname, port)
				.credentials(userEmail, password);

		try {
			getRemoteApiInstaller().install(getRemoteApiOptions());
			for(EntityUnit entityUnit :entityMapList ){
				Entity entity = new Entity(entityName);
				List<EntityField> entityFieldList = entityUnit.getEntityFieldsToPersist();
				for(EntityField field : entityFieldList){
					entity.setProperty(field.getDatabaseColumnName(), Utils.getMappingType(field.getDatabaseColumnType(), field.getDatabaseColumnValue()));
				}
				entities.add(entity);
			}
		

		} catch (Exception exception) {

		} finally {
			getRemoteApiInstaller().uninstall();
		}

	}


	private Map<String, String> getMap(String[] header, String[] fields) {
		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < header.length; i++) {
			map.put(header[i], fields[i]);
		}

		return map;
	}

	public List<File> getFiles() {
		File file = new File(this.migrationFolder + "\\" + getFolderName());
		return Arrays.asList(file.listFiles());
	}

	public CountDownLatch getImporterCountLatch() {
		return importerCountLatch;
	}

	public void setImporterCountLatch(CountDownLatch importerCountLatch) {
		this.importerCountLatch = importerCountLatch;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getUserEmail() {
		return userEmail;
	}

	public void setUserEmail(String userEmail) {
		this.userEmail = userEmail;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public List<String> readFile(File file) {
		List<String> fileAsList = new ArrayList<String>();
		try {

			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line;
			while ((line = reader.readLine()) != null) {
				fileAsList.add(line);
			}
			reader.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fileAsList;

	}

	public void createSequence(String entityName, String entitySequenceName,
			String entitySequenceIdName) {

		remoteApiOptions.server(hostname, port)
				.credentials(userEmail, password);

		try {
			getRemoteApiInstaller().install(getRemoteApiOptions());

			DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
			Query query = new Query(entityName);
			PreparedQuery preparedQuery = ds.prepare(query);
			Set<Integer> idSet = new HashSet<Integer>();
			for (Entity entity : preparedQuery.asIterable()) {
				String idMax = String.valueOf(entity.getProperty("id"));
				if(idMax==null || idMax.equals("null")){
					return;
				}
				idSet.add(Integer.valueOf(idMax));
			}
			Entity entity = new Entity(entitySequenceName);
			entity.setProperty(entitySequenceIdName, Collections.max(idSet));
			ds.put(entity);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			getRemoteApiInstaller().uninstall();

		}

	}

}
