package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpTimeDecorator;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.business.CcpBusiness;
import com.ccp.constants.CcpOtherConstants;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * GCP Pub/Sub {@code MessageReceiver}. Parses the received message as JSON, runs the task of the subscription over it and
 * acknowledges it ({@code ack}); on a failure, notifies the error handler and rejects it ({@code nack}), so Pub/Sub
 * delivers it again. Either way, writes one line about the message (topic, result, duration, wait in the queue) through
 * {@link CcpPubSubWriteMessageLog}.
 * <p>The task comes from outside because this module belongs to the ccp cost center, which knows nothing of how the
 * messages are processed: that belongs to whoever uses it. Until 2026-10-06 the task was commented out
 * and every valid message was acknowledged without being processed.</p>
 */
public class CcpMessageReceiver implements MessageReceiver {
	/** Fields of the error message and of the line of each message. */
	enum JsonFieldNames implements CcpJsonFieldName{
		/** The message that failed. */
		values,
		/** When the processing started, in the America/Sao_Paulo time zone. */
		startedAt,
		/** How long the processing took, in milliseconds. */
		durationMs,
		/** How long the message waited between its publication and the start of its processing, in milliseconds. */
		waitMs,
		/** The class of the failure. */
		errorClass,
		/** The text of the failure, cut to {@link CcpMessageReceiver#MAXIMUM_ERROR_MESSAGE_LENGTH} characters. */
		errorMessage
	}

	/** The texts of this class. */
	enum Constants implements CcpJsonFieldName {
		/** The format of {@code startedAt}. */
		START_FORMAT { public String getValue() { return "yyyy-MM-dd HH:mm:ss.SSS"; } }
	}

	/** The longest failure text kept in the line of a message. */
	private static final int MAXIMUM_ERROR_MESSAGE_LENGTH = 1000;

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
	 * Processes the message and writes its line. The line is written after the acknowledgement or the rejection, so
	 * a failure to write never changes the fate of the message.
	 * @param message the Pub/Sub message
	 * @param consumer acknowledges or rejects the message
	 */
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		long startedAt = System.currentTimeMillis();
		CcpJsonRepresentation result = this.process(message, consumer);
		long finishedAt = System.currentTimeMillis();
		long durationMs = finishedAt - startedAt;
		Timestamp publishTime = message.getPublishTime();
		long publishedAtSeconds = publishTime.getSeconds();
		int publishedAtNanos = publishTime.getNanos();
		long publishedAt = publishedAtSeconds * 1000 + publishedAtNanos / 1_000_000;
		long waitMs = startedAt - publishedAt;
		String startFormat = Constants.START_FORMAT.getValue();
		CcpTimeDecorator startedAtDecorator = new CcpTimeDecorator(startedAt);
		String formattedStart = startedAtDecorator.getFormattedDateTime(startFormat);
		String messageId = message.getMessageId();
		CcpJsonRepresentation line = result
				.put(CcpJsonCommonsFields.topic, this.name)
				.put(CcpJsonCommonsFields.message_id, messageId)
				.put(JsonFieldNames.startedAt, formattedStart)
				.put(JsonFieldNames.durationMs, durationMs)
				.put(JsonFieldNames.waitMs, waitMs);
		CcpPubSubWriteMessageLog.INSTANCE.execute(line);
	}

	/**
	 * Parses the message, runs the task over it and acknowledges it; on a failure (invalid JSON, or the task failing), runs
	 * the error handler once over the error details and rejects the message.
	 * @param message the Pub/Sub message
	 * @param consumer acknowledges or rejects the message
	 * @return the result of the processing ({@code ok}, or {@code error} with the class and the text of the failure)
	 */
	private CcpJsonRepresentation process(PubsubMessage message, AckReplyConsumer consumer) {
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
			String ok = CcpJsonCommonsFields.ok.name();
			CcpJsonRepresentation success = CcpOtherConstants.EMPTY_JSON.put(CcpJsonCommonsFields.result, ok);
			return success;
		} catch (Throwable e) {
			CcpJsonRepresentation json = new CcpJsonRepresentation(e);
			this.notifyError.execute(json);
			consumer.nack();
			CcpJsonRepresentation failure = getFailure(e);
			return failure;
		}
	}

	/**
	 * The result of a failed message: the class and the text of the original failure, not of the wrapper that adds the
	 * topic and the message to it.
	 * @param e the failure
	 * @return the result with the class and the text of the failure
	 */
	private static CcpJsonRepresentation getFailure(Throwable e) {
		boolean wrapped = e instanceof CcpErrorMessageReceiverTaskFailed;
		Throwable cause = e.getCause();
		Throwable failure = wrapped ? cause : e;
		Class<? extends Throwable> failureClass = failure.getClass();
		String failureClassName = failureClass.getName();
		String failureText = String.valueOf(failure.getMessage());
		int failureTextLength = failureText.length();
		int keptLength = Math.min(failureTextLength, MAXIMUM_ERROR_MESSAGE_LENGTH);
		String keptText = failureText.substring(0, keptLength);
		String error = CcpJsonCommonsFields.error.name();
		CcpJsonRepresentation result = CcpOtherConstants.EMPTY_JSON
				.put(CcpJsonCommonsFields.result, error)
				.put(JsonFieldNames.errorClass, failureClassName)
				.put(JsonFieldNames.errorMessage, keptText);
		return result;
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
