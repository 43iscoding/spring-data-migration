package com.springframework.datamigration.exporter;

import java.util.List;
import java.util.Map;

import org.springframework.context.ApplicationContext;

public class DataExporter implements Runnable {


	private ApplicationContext context;
	
	public void run() {
	
	Map<String, TableExporter> tableExporterMap =	 context.getBeansOfType(TableExporter.class);
	
    new Thread( tableExporterMap.values().iterator().next()).start();
	
	
	
	}
	
	
	public ApplicationContext getContext() {
		return context;
	}

	public void setContext(ApplicationContext context) {
		this.context = context;
	}

	

}
