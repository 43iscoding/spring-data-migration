package datamigration.utils;

import datamigration.Migration;

import java.util.*;

public class Stats {

    private static Map<String,List<Long>> exportStats = new HashMap<String, List<Long>>();
    private static Map<String,List<Long>> importStats = new HashMap<String, List<Long>>();

    public static void reportExport(String table, long startTime) {
        long time = Utils.getTime() - startTime;
/*        if (Migration.debug()) {
            System.out.println("[" + table + "] Processed in " + time + "ms");
        }*/

        if (!exportStats.containsKey(table)) {
            exportStats.put(table, new ArrayList<Long>());
        }

        exportStats.get(table).add(time);
    }

    public static void reportImport(String table, long startTime, long delay) {
        long time = Utils.getTime() - startTime;
        if (Migration.debug()) {
            System.out.println("[" + table + "] Processed in " + time + "ms | Delay : " + delay + "ms");
        }

        if (!importStats.containsKey(table)) {
            importStats.put(table, new ArrayList<Long>());
        }

        importStats.get(table).add(time);
    }

    public static void printStats() {
        System.out.println("Export stats:");
        for (Map.Entry<String,List<Long>> entry : exportStats.entrySet()) {
            printEntry(entry.getKey(), entry.getValue());
        }
        System.out.println("Import stats:");
        for (Map.Entry<String,List<Long>> entry : importStats.entrySet()) {
            printEntry(entry.getKey(), entry.getValue());
        }
    }

    private static void printEntry(String table, List<Long> v) {
        StringBuilder s = new StringBuilder("* [");
        s.append(table).append("]").append("x").append(v.size());
        s.append(": max(").append(max(v)).append(")");
        s.append(", min(").append(min(v)).append(")");
        s.append(", avg(").append(avg(v)).append(")");
        System.out.println(s.toString());
    }

    private static long max(List<Long> values) {
        return Collections.max(values);
    }

    private static long min(List<Long> values) {
        return Collections.min(values);
    }

    private static double avg(List<Long> values) {
        double avg = 0;
        for (Long value : values) {
            avg += value;
        }
        return avg / values.size();
    }
}
