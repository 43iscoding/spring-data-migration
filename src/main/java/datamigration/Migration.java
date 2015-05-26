package datamigration;

import datamigration.exporter.DataExporter;
import datamigration.importer.DataImporter;
import datamigration.utils.Utils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Migration {

	private static ApplicationContext ctx;
    private static Properties configurationProperties;

	public static void main(String args[]) {
		try {
            loadApplicationContext();
			System.out.println("***********************************************");
			System.out.println("Starting Data Migration");

            long timeStart = Utils.getTime();

            if (!getBoolProperty("skipExport")) {
                processExport();
            } else {
                System.out.println("WARNING : SKIPPING EXPORT");
            }

            if (!getBoolProperty("skipImport")) {
                processImport();
            } else {
                System.out.println("WARNING : SKIPPING IMPORT");
            }

			long timeFinish = Utils.getTime();

			System.out.println("Completed Data Migration in " + (timeFinish - timeStart) + "ms");
			System.out.println("***********************************************");

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    private static void processExport() throws InterruptedException {
        CountDownLatch latch1 = new CountDownLatch(1);
        DataExporter dataExporterThread = new DataExporter();
        dataExporterThread.setCountDownLatch(latch1);
        Thread t1 = new Thread(dataExporterThread);
        t1.start();
        latch1.await();

        System.out.println("Export finished!");
    }

    private static void processImport() throws InterruptedException {
        CountDownLatch latch2 = new CountDownLatch(1);
        DataImporter dataImporterThread = new DataImporter();
        dataImporterThread.setCountDownLatch(latch2);
        Thread t2 = new Thread(dataImporterThread);
        t2.start();
        latch2.await();

        System.out.println("Import finished!");
    }

	public static void loadApplicationContext() {
        ctx = new FileSystemXmlApplicationContext("src/main/java/datamigration/context.xml");
        configurationProperties = (Properties) ctx.getBean("config");
	}

    public static String getProperty(String name) {
        return configurationProperties.getProperty(name);
    }

    public static boolean getBoolProperty(String name) {
        return Boolean.valueOf(getProperty(name));
    }

    public static int getIntProperty(String name) {
        return Integer.valueOf(getProperty(name));
    }

    public static <T> T getBean(Class<T> clazz) {
        return ctx.getBean(clazz);
    }

    public static boolean debug() {
        return getBoolProperty("debug");
    }
}
