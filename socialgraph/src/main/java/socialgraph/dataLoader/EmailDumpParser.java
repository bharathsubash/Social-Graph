package socialgraph.dataLoader;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import info.debatty.java.datasets.enron.Dataset;
import info.debatty.java.datasets.enron.Email;
import utils.NotifyingBlockingThreadPoolExecutor;

public class EmailDumpParser {

	MongoClient mongo = new MongoClient("localhost", 27017);
	MongoDatabase database = mongo.getDatabase("social_graph");
	MongoCollection<Document> emailCollection = database.getCollection("email");

	public static void main(String[] args) throws Exception {
		EmailDumpParser edParser = new EmailDumpParser();
		edParser.process("D:\\bsd\\enron\\mail\\maildir\\1");
	}

	private void process(String path) throws Exception {
		Long startTime = System.currentTimeMillis();
		System.out.println("Loading started for " + path);
		ExecutorService executorService = new NotifyingBlockingThreadPoolExecutor(10, 20, 1, TimeUnit.SECONDS);
		int total = 0, updated = 0;

		ArrayList<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();

		for (File file : (new File(path)).listFiles()) {
			if (file.isDirectory()) {
				String absPath = file.getAbsolutePath();
				Dataset enron_dataset = new Dataset(absPath);
				EmailUpdater updater = new EmailUpdater(enron_dataset, emailCollection);

				Future<Boolean> result = executorService.submit(updater);
				futures.add(result);
			}
		}
		executorService.shutdown();

		while (!executorService.isTerminated()) {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		Long toTime = System.currentTimeMillis();
		System.out.println("Loading Completed for " + path + " in " + (toTime - startTime) + " milli seconds.");
	}
}

class EmailUpdater implements Callable<Boolean> {
	private Dataset ds;
	private MongoCollection<Document> emailCollection;

	public EmailUpdater(Dataset dataset, MongoCollection<Document> collection) {
		ds = dataset;
		emailCollection = collection;
	}

	public Boolean call() throws Exception {
		Long start = System.currentTimeMillis();
		Boolean isInserted = false;
		List<WriteModel<Document>> models = new LinkedList();
		for (Email email : ds) {
//			models.add(new InsertOneModel<Document>(email.getDoc()));
			UpdateOneModel<Document> upsertData = new UpdateOneModel<>(new Document("message_id", email.getMessageID()),new Document("$set",email.getDoc()),new UpdateOptions().upsert(true));
			models.add(upsertData);
			if (models.size() == 100) {
				try {
					emailCollection.bulkWrite(models, new BulkWriteOptions().ordered(false));
				} catch (Exception e) {
					e.printStackTrace();
				}
				models = new LinkedList();
			}
		}
		if (models.size() > 0) {
			try {
				emailCollection.bulkWrite(models, new BulkWriteOptions().ordered(false));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		Long end= System.currentTimeMillis();
		System.out.println("Time taken for " + ds + " = " + (end-start));
		return isInserted;
	}

}
