package datamigration.importer;

import com.google.appengine.api.datastore.*;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;
import datamigration.utils.Status;
import datamigration.utils.Utils;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class TableImporter implements Runnable {

    private String migrationFolder;
    private CountDownLatch importerCountLatch;

    private String tableToExport;
    private int entitiesExportCount;

    private RemoteApiInstaller remoteApiInstaller;
    private RemoteApiOptions remoteApiOptions;

	private String entityName;
	private String folderName;
	private String hostname;
    private int port;

    private String userEmail;
    private String password;

	/**
	 * The run() method contains the workflow logic for exporting the CSV files into
	 * GAE Datastore as entities and also for logging the result of export to the
	 * database.
	 */
	public void run() {
		System.out.println("Import started for Entity [ " + tableToExport + " ]");
        long startTime = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();
		List<File> files = getFiles();
		try {
			exportToAppEngineDataStore(files);
			updateExecutionStatus(entityName, entitiesExportCount,Status.SUCCESS, startTime);
			importerCountLatch.countDown();			
		} catch(Exception e){
			e.printStackTrace();
			updateExecutionStatus(entityName, entitiesExportCount,Status.FAILURE, startTime);
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

		for (int i = 0; i < files.size(); i++) {
			List<String> lines = readFile(files.get(i));
			List<Entity> entities = retrieveEntities(lines);
			entitiesExportCount += entities.size();
            System.out.println("Processing import for [ " + tableToExport + " ] - " + (i + 1) + "/" + files.size() + " CSV files");
			saveToDataStore(entities);
        }
		createSequence(entityName, Utils.createEntitySequenceName(entityName),
				Utils.createEntitySequenceId(entityName));
	}
	
	/**
	 * The method  update the status of the import of the table records in the GAE Datastore.
	 * 
	 * @param entityName- The name of the Entity Kind underwhich the entites are stored.
	 * @param count - The number of entites that are created for a particular Entity Kind.
	 * @param status - The status of the import of the table records in to GAE Datastore
	 * @param time - Start time of import
	 */	
	private void updateExecutionStatus(final String entityName,
			final Integer count, final Status status, final long time) {
        long timeFinish = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();
        System.out.println("Import finished for [ " + entityName +
                " ] (" + count + " entries). STATUS = " + status + " (" + (timeFinish - time) + "ms)");
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
            remoteApiInstaller.install(remoteApiOptions);
			DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
			ds.put(entities);
		} catch (IOException e) {
            System.out.println("Error while saving to data store: " + e.getMessage());
            throw e;
		} finally {
            remoteApiInstaller.uninstall();
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
            remoteApiInstaller.install(remoteApiOptions);
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
            remoteApiInstaller.uninstall();
		}
	}

	/**
	 * The method populates the list 'entities' with GAE Datastore Entity objects.
	 */
	public void createEntities(List<Entity> entities,List<EntityUnit> entityMapList) {
        remoteApiOptions.server(hostname, port).credentials(userEmail, password);
		try {
            remoteApiInstaller.install(remoteApiOptions);
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
            remoteApiInstaller.uninstall();
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
		File file = new File(this.migrationFolder + "\\" + folderName);

        File[] files = file.listFiles();
        if (files == null) return Collections.emptyList();

        return Arrays.asList(files);
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
