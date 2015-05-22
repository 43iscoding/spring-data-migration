package datamigration.utils;

import java.util.List;

public class Mapper {
    private String tableName;
    private String entityName;

    private List<String> fieldsToIgnore;

    public String getTableName() {
        return tableName;
    }

    public String getFolderName() {
        return tableName.toUpperCase();
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public List<String> getFieldsToIgnore() {
        return fieldsToIgnore;
    }

    public void setFieldsToIgnore(List<String> fieldsToIgnore) {
        this.fieldsToIgnore = fieldsToIgnore;
    }
}
