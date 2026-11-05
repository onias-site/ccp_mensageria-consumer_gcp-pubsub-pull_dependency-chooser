package com.ccp.topic.consumer.pubsub.pull;

import java.util.ArrayList;
import java.util.List;

import com.ccp.business.CcpBusiness;
import com.ccp.decorators.CcpFileDecorator;
import com.ccp.decorators.CcpReflectionConstructorDecorator;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;
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

	/** The optional column of a line that names the provider of the {@code CcpMensageriaSender} of its consumer. */
	private static final int SENDER_CLASS_COLUMN = 3;

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
	 * Starts the consumer of one line of the file of topics: topic, threads, ack deadline (read by {@code 03-criar-filas})
	 * and, optionally, {@code senderClass}.
	 * @param line the line
	 * @return the thread of the consumer
	 */
	private static Thread startConsumer(String line) {
		String columnSeparator = Constants.COLUMN_SEPARATOR.getValue();
		String[] columns = line.split(columnSeparator);
		String topic = columns[0];
		String threadsAsText = columns[1];
		int threads = Integer.parseInt(threadsAsText);
		CcpPubSubReceiverTask receiverTask = new CcpPubSubReceiverTask(topic);
		CcpBusiness task = getTask(columns, receiverTask);
		CcpPubSubTopicConsumer topicConsumer = new CcpPubSubTopicConsumer(topic, threads, task);
		Thread consumer = new Thread(topicConsumer, topic);
		consumer.start();
		return consumer;
	}

	/**
	 * The task of the consumer of a line. Without the column {@code senderClass}, the task publishes through the sender of
	 * the registry (the synchronous one: what it publishes runs in the same thread). With it, the provider named there is
	 * instantiated by {@link CcpDependencyInjection#getInstance}, without being registered, and every message of this
	 * consumer runs seeing that sender only in its own thread, so the choice does not reach the other consumers (since
	 * 2026-11-05, for the queues of the bots, which publish through Pub/Sub).
	 * @param columns the columns of the line
	 * @param receiverTask runs each message
	 * @return the task
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static CcpBusiness getTask(String[] columns, CcpPubSubReceiverTask receiverTask) {
		boolean senderOfTheRegistry = columns.length <= SENDER_CLASS_COLUMN;
		if (senderOfTheRegistry) {
			return receiverTask;
		}
		String senderClassName = columns[SENDER_CLASS_COLUMN];
		CcpStringDecorator senderClassNameDecorator = new CcpStringDecorator(senderClassName);
		CcpReflectionConstructorDecorator senderClassReflection = senderClassNameDecorator.reflection();
		Class providerClass = senderClassReflection.forName();
		CcpMensageriaSender sender = CcpDependencyInjection.getInstance((Class<CcpInstanceProvider<CcpMensageriaSender>>) providerClass);
		CcpPubSubTaskWithOwnSender taskWithOwnSender = new CcpPubSubTaskWithOwnSender(receiverTask, sender);
		return taskWithOwnSender;
	}
}
