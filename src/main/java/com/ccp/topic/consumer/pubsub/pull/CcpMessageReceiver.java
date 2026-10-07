package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.business.CcpBusiness;
import com.ccp.constants.CcpOtherConstants;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * GCP Pub/Sub {@code MessageReceiver}. Parses the received message as JSON, runs the task of the subscription over it and
 * acknowledges it ({@code ack}); on a failure, notifies the error handler and rejects it ({@code nack}), so Pub/Sub
 * delivers it again.
 * <p>The task comes from outside because this module belongs to the ccp cost center and the processing of the messages
 * (e.g. {@code JnMensageriaReceiver.executeProcess}) belongs to the jn one. Until 2026-10-06 the task was commented out
 * and every valid message was acknowledged without being processed.</p>
 */
public class CcpMessageReceiver implements MessageReceiver {
	/** Fields of the error message. */
	enum JsonFieldNames implements CcpJsonFieldName{
		/** The message that failed. */
		values
	}
	
	/** Handler of the failures of the receiver. */
	private final CcpBusiness notifyError ;

	/** Processes each message of the subscription. */
	private final CcpBusiness task;
	
	/** The subscription (topic) name. */
	public final String name;

	/**
	 * Builds the receiver.
	 * @param notifyError handler of the failures
	 * @param name the subscription name
	 * @param task processes each message of the subscription
	 */
	public CcpMessageReceiver(CcpBusiness notifyError, String name, CcpBusiness task) {
		this.notifyError = notifyError;
		this.name = name;
		this.task = task;
	}

	/**
	 * Parses the message, runs the task over it and acknowledges it; on a failure (invalid JSON, or the task failing), runs
	 * the error handler once over the error details and rejects the message.
	 * @param message the Pub/Sub message
	 * @param consumer acknowledges or rejects the message
	 */
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		try {
			ByteString data = message.getData();
			String receivedMessage = data.toStringUtf8();
			CcpJsonRepresentation messageJson = new CcpJsonRepresentation(receivedMessage);
			try {
				this.task.execute(messageJson);
			} catch (Throwable e) {
				CcpErrorMessageReceiverTaskFailed ccpErrorMessageReceiverTaskFailed = new CcpErrorMessageReceiverTaskFailed(this.name, messageJson, e);
				throw ccpErrorMessageReceiverTaskFailed;
			}
			consumer.ack();
		} catch (Throwable e) {
			CcpJsonRepresentation json = new CcpJsonRepresentation(e);
			this.notifyError.execute(json);
			consumer.nack();
		}

	}

	/** Raised when the task of a message fails. */
	@SuppressWarnings("serial")
	private static class CcpErrorMessageReceiverTaskFailed extends RuntimeException {
		/**
		 * Builds the error with the topic and the message.
		 * @param topicName the topic
		 * @param messageJson the message
		 * @param cause the original failure
		 */
		private CcpErrorMessageReceiverTaskFailed(String topicName, CcpJsonRepresentation messageJson, Throwable cause) {
			super(CcpOtherConstants.EMPTY_JSON
					.put(CcpJsonCommonsFields.topic, topicName)
					.put(JsonFieldNames.values, messageJson)
					.asPrettyJson(), cause);
		}
	}
}
