package com.ccp.topic.consumer.pubsub.pull;

import java.util.ArrayList;
import java.util.List;

import com.ccp.decorators.CcpFileDecorator;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.implementations.cache.gcp.memorystore.CcpGcpMemoryStore;
import com.ccp.implementations.db.bulk.elasticsearch.CcpElasticSerchDbBulk;
import com.ccp.implementations.db.crud.elasticsearch.CcpElasticSearchCrud;
import com.ccp.implementations.db.query.elasticsearch.CcpElasticSearchQueryExecutor;
import com.ccp.implementations.db.utils.elasticsearch.CcpElasticSearchDbRequest;
import com.ccp.implementations.email.sendgrid.CcpSendGridEmailProvider;
import com.ccp.implementations.file.bucket.gcp.CcpGcpFileBucket;
import com.ccp.implementations.http.apache.mime.CcpApacheMimeHttp;
import com.ccp.implementations.instant.messenger.telegram.CcpTelegramInstantMessenger;
import com.ccp.implementations.json.gson.CcpGsonJsonHandler;
import com.ccp.implementations.main.authentication.gcp.oauth.CcpGcpMainAuthentication;
import com.ccp.implementations.password.mindrot.CcpMindrotPasswordHandler;
import com.ccp.local.testings.implementations.CcpLocalInstances;

/**
 * The pull consumers of production, one process for every topic. Reads the file named by the first argument, whose lines
 * hold the topic and the number of threads separated by a tab, and keeps one subscriber per line until the process ends.
 * The business classes come at runtime: each message names its own receiver. What a process sends to the messaging runs
 * in the same thread: publishing it would only bring the work back to these consumers.
 */
public class CcpPubSubConsumersStarter {

	/** The texts of the file of topics. */
	enum Constants implements CcpJsonFieldName {
		/** What separates the columns of a line. */
		COLUMN_SEPARATOR { public String getValue() { return "\t"; } }
	}

	/**
	 * Wires the production dependencies, starts the consumers and waits for them.
	 * @param args the path of the file of topics
	 * @throws InterruptedException when the wait is interrupted
	 */
	public static void main(String[] args) throws InterruptedException {
		CcpGsonJsonHandler jsonHandler = CcpGsonJsonHandler.INSTANCE;
		CcpDependencyInjection.loadAllDependencies(jsonHandler);
		CcpDependencyInjection.loadAllDependencies(
				CcpLocalInstances.syncMensageriaListener,
				CcpGcpMemoryStore.INSTANCE,
				CcpGcpFileBucket.INSTANCE,
				CcpSendGridEmailProvider.INSTANCE,
				CcpTelegramInstantMessenger.INSTANCE,
				CcpElasticSearchDbRequest.INSTANCE,
				CcpMindrotPasswordHandler.INSTANCE,
				CcpGcpMainAuthentication.INSTANCE,
				CcpElasticSerchDbBulk.INSTANCE,
				CcpElasticSearchCrud.INSTANCE,
				CcpElasticSearchQueryExecutor.INSTANCE,
				CcpApacheMimeHttp.INSTANCE
		);

		String topicsFilePath = args[0];
		CcpStringDecorator topicsFilePathDecorator = new CcpStringDecorator(topicsFilePath);
		CcpFileDecorator topicsFile = topicsFilePathDecorator.file();
		List<String> lines = topicsFile.getLines();
		List<Thread> consumers = new ArrayList<>();
		for (String line : lines) {
			boolean blankLine = line.isBlank();
			if (blankLine) {
				continue;
			}
			Thread consumer = startConsumer(line);
			consumers.add(consumer);
		}
		for (Thread consumer : consumers) {
			consumer.join();
		}
	}

	/**
	 * Starts the consumer of one line of the file of topics.
	 * @param line the topic and the number of threads
	 * @return the thread of the consumer
	 */
	private static Thread startConsumer(String line) {
		String columnSeparator = Constants.COLUMN_SEPARATOR.getValue();
		String[] columns = line.split(columnSeparator);
		String topic = columns[0];
		String threadsAsText = columns[1];
		int threads = Integer.parseInt(threadsAsText);
		CcpPubSubTopicConsumer topicConsumer = new CcpPubSubTopicConsumer(topic, threads);
		Thread consumer = new Thread(topicConsumer, topic);
		consumer.start();
		return consumer;
	}
}
