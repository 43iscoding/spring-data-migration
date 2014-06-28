package com.springframework.datamigration.importer;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.appengine.api.datastore.Entity;

public class OwnersTableImporter extends TableImporter {

	@Override
	public void exportToAppEngineDataStore(List<File> files) {

		for (File file : files) {
			List<String> lines = readFile(file);
			List<Entity> entities = retrieveEntities(lines);
			saveToDataStore(entities);
		}

	}

	public void createEntities(List<Entity> entities,
			List<Map<String, String>> entityMapList) {

		try {
			getRemoteApiInstaller().install(getRemoteApiOptions());

			for (int i = 0; i < entityMapList.size(); i++) {

				Map<String, String> map = entityMapList.get(i);
				Entity entity = new Entity("Owner");
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
