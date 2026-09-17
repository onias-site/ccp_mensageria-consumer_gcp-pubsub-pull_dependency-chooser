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
 * Implementação de {@code MessageReceiver} do GCP Pub/Sub. Desserializa a mensagem recebida,
 * executa a tarefa assíncrona configurada e confirma ({@code ack}) em caso de sucesso ou
 * recusa ({@code nack}) notificando o handler de erros em caso de falha.
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
				CcpErrorMessageReceiverTaskFailed ccpErrorMessageReceiverTaskFailed = new CcpErrorMessageReceiverTaskFailed(this.name, mdMessage, e);
				throw ccpErrorMessageReceiverTaskFailed;
			}
			consumer.ack();
		} catch (Throwable e) {
			CcpJsonRepresentation json = new CcpJsonRepresentation(e);
			
			CcpJsonRepresentation execute = this.notifyError.execute(json);
			this.notifyError.execute(execute);
			consumer.nack();
		}

	}

	@SuppressWarnings("serial")
	private static class CcpErrorMessageReceiverTaskFailed extends RuntimeException {
		private CcpErrorMessageReceiverTaskFailed(String topicName, CcpJsonRepresentation mdMessage, Throwable cause) {
			super(CcpOtherConstants.EMPTY_JSON
					.put(CcpJsonCommonsFields.topic, topicName)
					.put(JsonFieldNames.values, mdMessage)
					.asPrettyJson(), cause);
		}
	}
}
