package com.springframework.datamigration.importer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.Entity;

public class PetsTableImporter extends TableImporter {


	private static String ENTITY_NAME = "Pet";
	
	private static String ENTITY_SEQUENCE = ENTITY_NAME.concat("Sequence");
	
	private static String ENTITY_SEQUENCE_ID = ENTITY_NAME.concat(ENTITY_SEQUENCE).concat("Id");
	
	@Override
	public void exportToAppEngineDataStore(List<File> files) {

		for (File file : files) {
			List<String> lines = readFile(file);
			List<Entity> entities = retrieveEntities(lines);
			saveToDataStore(entities);
		}
		createSequence(ENTITY_NAME, ENTITY_SEQUENCE, ENTITY_SEQUENCE_ID);
		
	}

	@Override
	public void createEntities(List<Entity> entities,
			List<Map<String, String>> entityMapList) {
		
		try {
			getRemoteApiInstaller().install(getRemoteApiOptions());

			for (int i = 0; i < entityMapList.size(); i++) {

				Map<String, String> map = entityMapList.get(i);
				Entity entity = new Entity(ENTITY_NAME);
				entity.setProperty("id", map.get("id"));
				entity.setProperty("name", map.get("name"));
				entity.setProperty("birthDate", map.get("birth_date"));
				entity.setProperty("typeId", map.get("type_id"));
				entity.setProperty("ownerId", map.get("owner_id"));
				entities.add(entity);

			}

		} catch (IOException e) {

		} finally {
			getRemoteApiInstaller().uninstall();
		}

	}

	
	
}
