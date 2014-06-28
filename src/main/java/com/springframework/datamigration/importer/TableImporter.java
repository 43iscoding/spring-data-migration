package com.springframework.datamigration.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.springframework.beans.factory.annotation.Value;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.remoteapi.RemoteApiInstaller;
import com.google.appengine.tools.remoteapi.RemoteApiOptions;

public abstract class TableImporter implements  Runnable {
	
	protected RemoteApiInstaller remoteApiInstaller;	
	
	private RemoteApiOptions remoteApiOptions;

	private String folderName;
	

	@Value("${hostname}")
	private String hostname;
	
	@Value("${port}")
	private int port;

	@Value("${userEmail}")
	private String userEmail;
	
	@Value("${password}")
	private String password;
	

	private  CountDownLatch importerCountLatch;
	
	
	
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

	
	public void run() {
	   List<File> files = getFiles();
	   try {

	
	   exportToAppEngineDataStore(files);
	   
	   importerCountLatch.countDown();
	   }finally{
		   
	   }
	   
	}
	
	public abstract void exportToAppEngineDataStore(List<File> files);


	public void initialization(){
	
			remoteApiOptions.server(hostname, port).credentials(userEmail, password);
			
	}
	
	
	public void saveToDataStore(List<Entity> entities) {
		
		
			
		try {
			getRemoteApiInstaller().install(getRemoteApiOptions());
			 DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
			 for(Entity entity : entities){
				 ds.put(entity);
			 }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			getRemoteApiInstaller().uninstall();
		}

			
			
	}
	

	public List<Entity> retrieveEntities(List<String> lines) {
		List<Entity> entities = new ArrayList<Entity>();
		List<Map<String,String>> entityMapList = new ArrayList<Map<String,String>>();
		if(lines.size()>1){
			String[] header =  lines.get(0).split(",");
			for(int i=1;i<lines.size();i++){
				String[] fields =  lines.get(i).split(",");
				entityMapList.add(getMap(header,fields )) ;
			}
			createEntities(entities,entityMapList);
		}
		return entities;
	}

	public abstract void createEntities(List<Entity> entities,	List<Map<String, String>> entityMapList);
	
	
	private Map<String,String> getMap(String[] header, String[] fields) {
		Map<String, String> map = new HashMap<String, String>();
	   for(int i=0;i<header.length;i++){
		   map.put(header[i], fields[i]);
	   }
		
		return map;
	}
	
	
   public List<File> getFiles(){
	   File file = new File(this.migrationFolder+ "\\"+ getFolderName());
	   return  Arrays.asList(file.listFiles());
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

	

	public List<String> readFile(File file){
		List<String> fileAsList = new ArrayList<String>();
		try {
			
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line;
			while( (line = reader.readLine())!=null){
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
	
	
	
}
