package datamigration.utils;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MapperConfig {
    private List<Mapper> mappers;

    public List<Mapper> getMappers() {
        return mappers;
    }

    public void setMappers(List<Mapper> mappers) {
        this.mappers = mappers;
    }

    public CountDownLatch getCountDownLatch() {
        return new CountDownLatch(mappers.size() - 1);
    }

    public boolean shouldProcess(String tableName) {
        for (Mapper mapper : mappers) {
            if (mapper.getTableName().equals(tableName.toUpperCase())) return true;
        }
        return false;
    }
}
