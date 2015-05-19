package datamigration.importer;

import com.google.appengine.api.datastore.*;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;
import datamigration.utils.Status;
import datamigration.utils.Utils;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class TableImporter implements Runnable {

	private int entitiesExportCount;

	private String entityName;
	
	private String folderName;

	private String hostname;

	private CountDownLatch importerCountLatch;

	private JdbcTemplate jdbcTemplate;

	protected String migrationFolder;

	private String password;

	private int port;	

	protected RemoteApiInstaller remoteApiInstaller;

	private RemoteApiOptions remoteApiOptions;

	private String tableToExport;

	private String userEmail;
	
	/**
	 * The run() method contains the workflow logic for exporting the CSV files into
	 * GAE Datastore as entities and also for logging the result of export to the
	 * database.
	 */
	public void run() {
		System.out
				.println("Creating App Engine Datastore Entities for table [ "
						+ tableToExport + " ] from the CSV files");
		List<File> files = getFiles();
		try {
			exportToAppEngineDataStore(files);
			updateExecutionStatus(entityName,entitiesExportCount,Status.SUCCESS,new Date());
			System.out.println("Completed Creation of App Engine Datastore Entities for table [ "
                    + tableToExport + " ] from the CSV files");
			importerCountLatch.countDown();			
		} catch(Exception e){
			e.printStackTrace();
			updateExecutionStatus(entityName,entitiesExportCount,Status.FAILURE,new Date());
		} 
	}
	
	/**
	 * The method reads the CSV files and create GAE Entities. 
	 * It also creates a Sequence Entity object for each GAE Entity Kind to serve as Sequence
	 * 
	 * @param files - The list of file objects related to a table
	 * @throws IOException - Throws an IOException if a file is not found
	 */
	public void exportToAppEngineDataStore(List<File> files) throws IOException {
		for (File file : files) {
			List<String> lines = readFile(file);
			List<Entity> entities = retrieveEntities(lines);
			entitiesExportCount += entities.size();
			saveToDataStore(entities);
		}
		createSequence(entityName, Utils.createEntitySequenceName(entityName),
				Utils.createEntitySequenceId(entityName));
	}
	
	/**
	 * The method  update the status of the import of the table records in the GAE Datastore.
	 * 
	 * @param entityName- The name of the Entity Kind underwhich the entites are stored.
	 * @param entitiesExportCount - The number of entites that are created for a particular Entity Kind.
	 * @param status - The status of the import of the table records in to GAE Datastore
	 * @param date   - The date on which the table is imported.
	 */	
	private void updateExecutionStatus(final String entityName,
			final Integer entitiesExportCount,final Status status,final Date date) {		
		final String INSERT_SQL = "INSERT INTO DATA_IMPORT_RESULT ("
				+ "ENTITY_NAME,"
				+ "ENTITIES_CREATED_COUNT,"
				+ "ENTITIES_CREATION_STATUS,"
				+ "ENTITIES_CREATION_DATE) VALUES (?,?,?,?)";		
		jdbcTemplate.update(INSERT_SQL, entityName,entitiesExportCount,status.name(),new java.sql.Date(date.getTime()));
	}
	
	/**
	 * The method uses remote api's to access the Datastore service for inserting objects into  GAE Datastore.
	 * 
	 * @param entities - The entities to be store in GAE Datastore
	 * @throws IOException
	 */
	public void saveToDataStore(List<Entity> entities) throws IOException {
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
	
	/**
	 * The method creates an Sequence Entity Kind for each Entity in the Datastore. 
	 * @param entityName - Entity Name.
	 * @param entitySequenceName - Name of the Sequence Entity Kind.
	 * @param entitySequenceIdName - The property name to hold the value of the sequence.
	 */
	public void createSequence(String entityName, String entitySequenceName,
			String entitySequenceIdName) {
		remoteApiOptions.server(hostname, port).credentials(userEmail, password);
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

	/**
	 * The method populates the list 'entities' with GAE Datastore Entity objects.
	 * @param entities
	 * @param entityMapList
	 */
	public void createEntities(List<Entity> entities,List<EntityUnit> entityMapList) {
        remoteApiOptions.server(hostname, port).credentials(userEmail, password);
		try {
			getRemoteApiInstaller().install(getRemoteApiOptions());
			for(EntityUnit entityUnit :entityMapList ){
				Entity entity = new Entity(entityName);
				List<EntityField> entityFieldList = entityUnit.getEntityFieldsToPersist();
				for(EntityField field : entityFieldList){
                    Object value = Utils.map(field.getDatabaseColumnType(), field.getDatabaseColumnValue());
                    if (value == Utils.ERROR) {
                        System.out.println("Error while processing: " + field);
                    } else {
                        entity.setProperty(field.getDatabaseColumnName(), value);
                    }
				}
				entities.add(entity);
			}
		} catch (Exception exception) {
            System.out.println("Exception while creating entities: " + exception);
            exception.printStackTrace();
        } finally {
			getRemoteApiInstaller().uninstall();
		}
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

	/**
	 * The method returns the list of Files from a migration folder.
	 * 
	 * @return List<File>
	 */
	public List<File> getFiles() {
		File file = new File(this.migrationFolder + "\\" + getFolderName());
        System.out.println("Get files from " + file);

        File[] files = file.listFiles();
        if (files == null) return Collections.emptyList();

        return Arrays.asList(files);
	}

	private Map<String, String> getMap(String[] header, String[] fields) {
		Map<String, String> map = new HashMap<String, String>();
		for (int i = 0; i < header.length; i++) {
			map.put(header[i], fields[i]);
		}
		return map;
	}
	
	/**
	 * The method reads a file and return it as a list of strings.
	 * @param file - The CSV file object.
	 * @return the file as a List<String>.
	 */
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
	
	/**
	 * @param lines - The lines in the CSV file containing the metadata and table records.
	 * @return List<Entity> to be saved into the GAE Datastore.
	 */
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

	public RemoteApiInstaller getRemoteApiInstaller() {
		return remoteApiInstaller;
	}

	public RemoteApiOptions getRemoteApiOptions() {
		return remoteApiOptions;
	}

	public String getTableToExport() {
		return tableToExport;
	}

	public String getUserEmail() {
		return userEmail;
	}

	public String getFolderName() {
		return folderName;
	}

	public String getHostname() {
		return hostname;
	}

	public CountDownLatch getImporterCountLatch() {
		return importerCountLatch;
	}

	public String getMigrationFolder() {
		return migrationFolder;
	}

	public String getPassword() {
		return password;
	}

	public int getPort() {
		return port;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public void setFolderName(String folderName) {
		this.folderName = folderName;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setImporterCountLatch(CountDownLatch importerCountLatch) {
		this.importerCountLatch = importerCountLatch;
	}

	public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	public void setMigrationFolder(String migrationFolder) {
		this.migrationFolder = migrationFolder;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setRemoteApiInstaller(RemoteApiInstaller remoteApiInstaller) {
		this.remoteApiInstaller = remoteApiInstaller;
	}	

	public void setRemoteApiOptions(RemoteApiOptions remoteApiOptions) {
		this.remoteApiOptions = remoteApiOptions;
	}

	public void setTableToExport(String tableToExport) {
		this.tableToExport = tableToExport;
	}

	public void setUserEmail(String userEmail) {
		this.userEmail = userEmail;
	}
}
