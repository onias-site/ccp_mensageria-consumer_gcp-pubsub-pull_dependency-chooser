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
 * GCP Pub/Sub {@code MessageReceiver} implementation. Deserializes the received message,
 * runs the configured asynchronous task and acknowledges it ({@code ack}) on success, or
 * rejects it ({@code nack}) and notifies the error handler on failure.
 */
public class CcpMessageReceiver implements MessageReceiver {
	enum JsonFieldNames implements CcpJsonFieldName{
		values
	}
	protected final CcpBusiness jnAsyncBusinessNotifyError;
	
	private final CcpBusiness notifyError ;

	protected final CcpEntity asyncTask;
	
	public final String name;

	
	public CcpMessageReceiver(CcpBusiness notifyError,
			 CcpEntity asyncTask,
			String name,  CcpBusiness jnAsyncBusinessNotifyError) {
		this.notifyError = notifyError;
		this.asyncTask = asyncTask;
		this.jnAsyncBusinessNotifyError = jnAsyncBusinessNotifyError;
		this.name = name;
	}

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

	@SuppressWarnings("serial")
	private static class CcpErrorMessageReceiverTaskFailed extends RuntimeException {
		private CcpErrorMessageReceiverTaskFailed(String topicName, CcpJsonRepresentation messageJson, Throwable cause) {
			super(CcpOtherConstants.EMPTY_JSON
					.put(CcpJsonCommonsFields.topic, topicName)
					.put(JsonFieldNames.values, messageJson)
					.asPrettyJson(), cause);
		}
	}
}
