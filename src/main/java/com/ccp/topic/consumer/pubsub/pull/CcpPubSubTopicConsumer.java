package com.ccp.topic.consumer.pubsub.pull;

/**
 * Keeps the subscriber of one topic. When the subscriber ends, which only a failure does, the whole process ends with an
 * error, so the process manager starts everything again instead of leaving a topic without a consumer in silence.
 */
class CcpPubSubTopicConsumer implements Runnable {

	/** The topic, which also names the subscription. */
	private final String topic;

	/** The number of threads of the subscriber. */
	private final int threads;

	/**
	 * Keeps the settings.
	 * @param topic the topic
	 * @param threads the number of threads of the subscriber
	 */
	CcpPubSubTopicConsumer(String topic, int threads) {
		this.topic = topic;
		this.threads = threads;
	}

	/** Subscribes and ends the process when the subscriber ends. */
	public void run() {
		CcpPubSubReceiverTask task = new CcpPubSubReceiverTask(this.topic);
		CcpMessageReceiver receiver = new CcpMessageReceiver(CcpPubSubLogFailure.INSTANCE, this.topic, task);
		CcpPubSubStarter starter = new CcpPubSubStarter(CcpPubSubLogFailure.INSTANCE, receiver, this.threads);
		starter.synchronizeMessages();
		System.exit(1);
	}
}
