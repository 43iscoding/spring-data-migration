package com.springframework.datamigration.importer;

import java.util.ArrayList;
import java.util.List;

public class EntityUnit {
	
	private List<EntityField> entityFieldsToPersist = new ArrayList<EntityField>();
	
	public List<EntityField> getEntityFieldsToPersist() {
		return entityFieldsToPersist;
	}
	
	public void addFieldToPersist( EntityField entityField ){
		entityFieldsToPersist.add(entityField);
	}

}
