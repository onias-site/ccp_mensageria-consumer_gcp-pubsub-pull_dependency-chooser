package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.business.CcpBusiness;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.especifications.mensageria.receiver.CcpMensageriaReceiver;

/**
 * Runs a message through the receiver that the message itself names in {@code mensageriaReceiver}. A failure goes up to
 * {@link CcpMessageReceiver}, which rejects the message so Pub/Sub delivers it again.
 */
class CcpPubSubReceiverTask implements CcpBusiness {

	/** The topic of the messages. */
	private final String topic;

	/**
	 * Keeps the topic.
	 * @param topic the topic of the messages
	 */
	CcpPubSubReceiverTask(String topic) {
		this.topic = topic;
	}

	/**
	 * Runs the message.
	 * @param json the message
	 * @return the message
	 */
	public CcpJsonRepresentation apply(CcpJsonRepresentation json) {
		CcpMensageriaReceiver receiver = CcpMensageriaReceiver.getInstance(json);
		receiver.executeProcess(this.topic, json);
		return json;
	}
}
