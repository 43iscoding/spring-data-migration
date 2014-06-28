package com.springframework.datamigration.importer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.Entity;

public class SpecialtiesTableImporter extends TableImporter {

	@Override
	public void exportToAppEngineDataStore(List<File> files) {
		for (File file : files) {
			List<String> lines = readFile(file);
			List<Entity> entities = retrieveEntities(lines);
			saveToDataStore(entities);
		}

	}

	@Override
	public void createEntities(List<Entity> entities,
			List<Map<String, String>> entityMapList) {


		try {
			getRemoteApiInstaller().install(getRemoteApiOptions());

			for (int i = 0; i < entityMapList.size(); i++) {
				Map<String, String> map = entityMapList.get(i);
				Entity entity = new Entity("Specialties");
				entity.setProperty("id", map.get("id"));
				entity.setProperty("name", map.get("name"));
				entities.add(entity);
			}

		} catch (IOException e) {

		} finally {
			getRemoteApiInstaller().uninstall();
		}

	

	}

}