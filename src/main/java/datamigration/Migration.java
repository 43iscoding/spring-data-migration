package datamigration;

import datamigration.exporter.DataExporter;
import datamigration.importer.DataImporter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;

public class Migration {

	private static ApplicationContext ctx;

	public static void main(String args[]) {
		try {
            loadApplicationContext();
			System.out.println("***********************************************");
			System.out.println("Starting Data Migration");
            System.out.println();

            Properties configurationProperties = (Properties) ctx.getBean("config");

            long timeStart = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();

            if (!Boolean.valueOf(configurationProperties.getProperty("skipExport"))) {
                CountDownLatch latch1 = new CountDownLatch(1);
                DataExporter dataExporterThread = new DataExporter();
                dataExporterThread.setContext(ctx);
                dataExporterThread.setCountDownLatch(latch1);
                Thread t1 = new Thread(dataExporterThread);
                t1.start();

                latch1.await();

                System.out.println();
                System.out.println("Export finished, proceed to import");
                System.out.println();
            } else {
                System.out.println("Skipping export, proceed to import");
                System.out.println();
            }

            CountDownLatch latch2 = new CountDownLatch(1);
			DataImporter dataImporterThread = new DataImporter();
			dataImporterThread.setContext(ctx);
			dataImporterThread.setCountDownLatch(latch2);
			Thread t2 = new Thread(dataImporterThread);
			t2.start();
			latch2.await();

			long timeFinish = Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();

			System.out.println();
			System.out.println("Completed Data Migration in " + (timeFinish - timeStart) + "ms");
			System.out.println("***********************************************");

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void loadApplicationContext() {
        ctx = new FileSystemXmlApplicationContext("src/main/java/datamigration/context.xml");
	}

}
