package com.springframework.datamigration.importer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;

public class OwnersTableImporter extends TableImporter {

	private static String ENTITY_NAME = "Owner";
	
	private static String ENTITY_SEQUENCE = ENTITY_NAME.concat("Sequence");
	
	private static String ENTITY_SEQUENCE_ID = ENTITY_NAME.concat(ENTITY_SEQUENCE).concat("Id");
	
	
	@Override
	public void exportToAppEngineDataStore(List<File> files) {

		for (File file : files) {
			List<String> lines = readFile(file);
			List<Entity> entities = retrieveEntities(lines);
			saveToDataStore(entities);
		}
		
		createSequence(ENTITY_NAME,ENTITY_SEQUENCE,ENTITY_SEQUENCE_ID);

	}

	

	public void createEntities(List<Entity> entities,
			List<Map<String, String>> entityMapList) {

		try {
			getRemoteApiInstaller().install(getRemoteApiOptions());

			for (int i = 0; i < entityMapList.size(); i++) {

				Map<String, String> map = entityMapList.get(i);
				Entity entity = new Entity(ENTITY_NAME);
				entity.setProperty("id", map.get("id"));
				entity.setProperty("firstName", map.get("first_name"));
				entity.setProperty("lastName", map.get("last_name"));
				entity.setProperty("address", map.get("address"));
				entity.setProperty("city", map.get("city"));
				entity.setProperty("telephone", map.get("telephone"));
				entities.add(entity);

			}

		} catch (IOException e) {

		} finally {
			getRemoteApiInstaller().uninstall();
		}

	}

}
