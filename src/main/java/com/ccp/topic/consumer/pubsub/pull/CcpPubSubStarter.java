package com.ccp.topic.consumer.pubsub.pull;

import java.io.InputStream;

import com.ccp.business.CcpBusiness;
import com.ccp.decorators.CcpInputStreamDecorator;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpJsonFieldName;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.Subscriber.Builder;
import com.google.pubsub.v1.ProjectSubscriptionName;
/**
 * Inicializador de assinante Pub/Sub pull para GCP. Lê as credenciais de
 * {@code GOOGLE_APPLICATION_CREDENTIALS}, cria o {@code Subscriber} com o número de threads
 * configurado e aguarda mensagens em {@code synchronizeMessages()}.
 */
public class CcpPubSubStarter {
	enum JsonFieldNames implements CcpJsonFieldName{
		project_id
	}

	final CcpJsonRepresentation parameters;
	
	private final CcpMessageReceiver topic;
	
	private final int threads;
	
	private final CcpBusiness notifyError ;
	
	
	public CcpPubSubStarter(CcpBusiness notifyError, CcpMessageReceiver topic, int threads) {
		this.parameters = this.loadCredentials();
		this.notifyError = notifyError;
		this.threads = threads;
		this.topic = topic;
	}

	private CcpJsonRepresentation loadCredentials() {
		CcpStringDecorator credentialsJson = new CcpStringDecorator("GOOGLE_APPLICATION_CREDENTIALS");
		CcpPropertiesDecorator propertiesFrom = credentialsJson.propertiesFrom();
		CcpJsonRepresentation environmentVariablesOrClassLoaderOrFile = propertiesFrom.environmentVariablesOrClassLoaderOrFile();
		return environmentVariablesOrClassLoaderOrFile;
	}
		
	public CcpPubSubStarter synchronizeMessages() {
		
		Subscriber subscriber = null;
		try {
			String projectName = this.parameters.getAsString(JsonFieldNames.project_id);
			
			ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectName, this.topic.name);
			InstantiatingExecutorProvider.Builder newBuilder2 = InstantiatingExecutorProvider.newBuilder();
			InstantiatingExecutorProvider.Builder setExecutorThreadCount = newBuilder2.setExecutorThreadCount(this.threads);
			ExecutorProvider executorProvider = setExecutorThreadCount.build();

			FixedCredentialsProvider credentials = this.getCredentials();
			
			Builder newBuilder = Subscriber.newBuilder(subscription, this.topic);
			Builder setCredentialsProvider = newBuilder.setCredentialsProvider(credentials);
			Builder setExecutorProvider = setCredentialsProvider.setExecutorProvider(executorProvider);
			subscriber = setExecutorProvider.build(); 
			subscriber.startAsync();
			subscriber.awaitTerminated();
			return this;
		}catch (IllegalStateException e) {
			Throwable cause2 = e.getCause();
			boolean isComgoogleapigaxrpcNotFoundException = cause2 instanceof com.google.api.gax.rpc.NotFoundException;
			if(isComgoogleapigaxrpcNotFoundException) {
				CcpErrorPubSubTopicNotCreated ex = new CcpErrorPubSubTopicNotCreated(this.topic.name);
				CcpJsonRepresentation json = new CcpJsonRepresentation(ex);
				
				CcpJsonRepresentation execute = this.notifyError.execute(json);
				this.notifyError.execute(execute);
			}
			return this;
		} catch (Throwable e) {
			CcpJsonRepresentation json = new CcpJsonRepresentation(e);
			
			CcpJsonRepresentation execute = this.notifyError.execute(json);
			this.notifyError.execute(execute);
			return this;
		} finally {
			boolean subscriberDiferente = subscriber != null;
			if (subscriberDiferente) {
				subscriber.stopAsync();
			}
		}
	}

	private FixedCredentialsProvider getCredentials(){
		CcpStringDecorator ccpStringDecorator = new CcpStringDecorator("GOOGLE_APPLICATION_CREDENTIALS");
		CcpInputStreamDecorator inputStreamFrom = ccpStringDecorator.inputStreamFrom();
		
		try (InputStream is = inputStreamFrom.fromEnvironmentVariablesOrClassLoaderOrFile(); ){
			ServiceAccountCredentials fromStream = ServiceAccountCredentials.fromStream(is);
			FixedCredentialsProvider create = FixedCredentialsProvider.create(fromStream);
			return create;
			
		} catch (Exception e) {
			CcpErrorPubSubCredentialsLoad ccpErrorPubSubCredentialsLoad = new CcpErrorPubSubCredentialsLoad(e);
			throw ccpErrorPubSubCredentialsLoad;		}


	
	}

	

	@SuppressWarnings("serial")
	private static class CcpErrorPubSubCredentialsLoad extends RuntimeException {
		private CcpErrorPubSubCredentialsLoad(Throwable cause) {
			super(cause);
		}
	}

	/**
	 * Exceção usada para relatar que a inscrição não pôde ser iniciada porque o tópico ainda não existe no PubSub.
	 */
	@SuppressWarnings("serial")
	public static class CcpErrorPubSubTopicNotCreated extends RuntimeException {
		/**
		 * Monta a mensagem informando qual tópico está faltando.
		 * @param topicName o nome do tópico ainda não criado
		 */
		private CcpErrorPubSubTopicNotCreated(String topicName) {
			super("Topic still has not been created: " + topicName);
		}
	}
}
