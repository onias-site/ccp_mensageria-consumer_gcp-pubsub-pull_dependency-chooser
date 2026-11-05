package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.business.CcpBusiness;

/**
 * Keeps the subscriber of one topic. When the subscriber ends, which only a failure does, the whole process ends with an
 * error, so the process manager starts everything again instead of leaving a topic without a consumer in silence.
 */
class CcpPubSubTopicConsumer implements Runnable {

	/** The topic, which also names the subscription. */
	private final String topic;

	/** The number of threads of the subscriber. */
	private final int threads;

	/** Runs each message. */
	private final CcpBusiness task;

	/**
	 * Keeps the settings.
	 * @param topic the topic
	 * @param threads the number of threads of the subscriber
	 * @param task runs each message
	 */
	CcpPubSubTopicConsumer(String topic, int threads, CcpBusiness task) {
		this.topic = topic;
		this.threads = threads;
		this.task = task;
	}

	/** Subscribes and ends the process when the subscriber ends. */
	public void run() {
		CcpMessageReceiver receiver = new CcpMessageReceiver(CcpPubSubLogFailure.INSTANCE, this.topic, this.task);
		CcpPubSubStarter starter = new CcpPubSubStarter(CcpPubSubLogFailure.INSTANCE, receiver, this.threads);
		starter.synchronizeMessages();
		System.exit(1);
	}
}
