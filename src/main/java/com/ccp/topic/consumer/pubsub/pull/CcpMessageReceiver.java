package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonRepresentation.CcpJsonFieldName;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.especifications.mensageria.receiver.CcpBusiness;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
public class CcpMessageReceiver implements MessageReceiver {
	enum JsonFieldNames implements CcpJsonFieldName{
		values, topic
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
			CcpJsonRepresentation mdMessage = new CcpJsonRepresentation(receivedMessage);
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
//				task.apply(mdMessage);
			} catch (Throwable e) {
				CcpJsonRepresentation put = CcpOtherConstants.EMPTY_JSON
						.put(JsonFieldNames.topic, this.name).put(JsonFieldNames.values, mdMessage);
				throw new RuntimeException(put.asPrettyJson(), e);
			}
			consumer.ack();
		} catch (Throwable e) {
			CcpJsonRepresentation json = new CcpJsonRepresentation(e);
			
			CcpJsonRepresentation execute = this.notifyError.apply(json);
			this.notifyError.apply(execute);
			consumer.nack();
		}
		
	}

}
