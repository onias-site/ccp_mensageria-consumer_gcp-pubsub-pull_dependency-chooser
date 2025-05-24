package com.ccp.topic.consumer.pubsub.pull;

import java.util.function.Function;

import com.ccp.constantes.CcpOtherConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.especifications.db.utils.CcpEntity;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

public class CcpMessageReceiver implements MessageReceiver {
	protected final Function<CcpJsonRepresentation, CcpJsonRepresentation> jnAsyncBusinessNotifyError;
	
	private final Function<CcpJsonRepresentation, CcpJsonRepresentation> notifyError ;

	protected final CcpEntity asyncTask;
	
	public final String name;

	
	public CcpMessageReceiver(Function<CcpJsonRepresentation, CcpJsonRepresentation> notifyError,
			 CcpEntity asyncTask,
			String name,  Function<CcpJsonRepresentation, CcpJsonRepresentation> jnAsyncBusinessNotifyError) {
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
/*				Function<CcpJsonRepresentation, CcpJsonRepresentation> task = msg -> 
 * 					CcpAsyncTask.executeProcess(this.name, msg, 
 * 					this.asyncTask, this.jnAsyncBusinessNotifyError);
*/
//				Function<CcpJsonRepresentation, CcpJsonRepresentation> task = msg -> 			
//				JnAsyncMensageriaSender.INSTANCE.executeProcesss(
//						this.asyncTask, 
//						this.name, 
//						msg, 
//						this.jnAsyncBusinessNotifyError
//						);
//				task.apply(mdMessage);
			} catch (Throwable e) {
				CcpJsonRepresentation put = CcpOtherConstants.EMPTY_JSON.put("topic", this.name).put("values", mdMessage);
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
