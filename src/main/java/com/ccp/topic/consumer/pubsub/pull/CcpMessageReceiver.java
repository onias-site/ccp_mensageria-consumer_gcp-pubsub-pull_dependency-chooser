package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.especifications.db.utils.entity.CcpEntity;
import com.ccp.business.CcpBusiness;
import com.ccp.constants.CcpOtherConstants;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.ccp.json.fields.validation.CcpJsonCommonsFields;

/**
 * GCP Pub/Sub {@code MessageReceiver}. Parses the received message as JSON and acknowledges it ({@code ack}); on a
 * failure, notifies the error handler and rejects it ({@code nack}). The execution of the asynchronous task is
 * commented out, so a valid message is acknowledged without being processed.
 */
public class CcpMessageReceiver implements MessageReceiver {
	/** Fields of the error message. */
	enum JsonFieldNames implements CcpJsonFieldName{
		/** The message that failed. */
		values
	}
	/** Error handler meant for the asynchronous task (unused while the task is disabled). */
	protected final CcpBusiness jnAsyncBusinessNotifyError;
	
	/** Handler of the failures of the receiver. */
	private final CcpBusiness notifyError ;

	/** Entity of the asynchronous tasks (unused while the task is disabled). */
	protected final CcpEntity asyncTask;
	
	/** The subscription (topic) name. */
	public final String name;

	
	/**
	 * Builds the receiver.
	 * @param notifyError handler of the failures
	 * @param asyncTask entity of the asynchronous tasks
	 * @param name the subscription name
	 * @param jnAsyncBusinessNotifyError error handler of the asynchronous task
	 */
	public CcpMessageReceiver(CcpBusiness notifyError,
			 CcpEntity asyncTask,
			String name,  CcpBusiness jnAsyncBusinessNotifyError) {
		this.notifyError = notifyError;
		this.asyncTask = asyncTask;
		this.jnAsyncBusinessNotifyError = jnAsyncBusinessNotifyError;
		this.name = name;
	}

	/**
	 * Parses and acknowledges the message; on a failure (e.g. invalid JSON), runs the error handler over the error details
	 * and then again over its own result, and rejects the message.
	 * @param message the Pub/Sub message
	 * @param consumer acknowledges or rejects the message
	 */
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		try {
			ByteString data = message.getData();
			String receivedMessage = data.toStringUtf8();
			CcpJsonRepresentation messageJson = new CcpJsonRepresentation(receivedMessage);
			try {
/*				CcpBusiness task = msg -> 
 * 					CcpAsyncTask.executeProcess(this.name, msg, 
 * 					this.asyncTask, this.jnAsyncBusinessNotifyError);
*/
//				CcpBusiness task = msg -> 			
//				JnAsyncMensageriaSender.INSTANCE.executeProcesss(
//						this.asyncTask, 
//						this.name, 
//						msg, 
//						this.jnAsyncBusinessNotifyError
//						);
//				task.apply(messageJson);
			} catch (Throwable e) {
				CcpErrorMessageReceiverTaskFailed ccpErrorMessageReceiverTaskFailed = new CcpErrorMessageReceiverTaskFailed(this.name, messageJson, e);
				throw ccpErrorMessageReceiverTaskFailed;
			}
			consumer.ack();
		} catch (Throwable e) {
			CcpJsonRepresentation json = new CcpJsonRepresentation(e);
			
			CcpJsonRepresentation errorNotificationResult = this.notifyError.execute(json);
			this.notifyError.execute(errorNotificationResult);
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
