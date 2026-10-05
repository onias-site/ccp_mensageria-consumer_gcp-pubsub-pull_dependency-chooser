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
 * GCP Pub/Sub pull subscriber starter. Reads the credentials from
 * {@code GOOGLE_APPLICATION_CREDENTIALS}, creates the {@code Subscriber} with the configured
 * number of threads and waits for messages in {@code synchronizeMessages()}.
 */
public class CcpPubSubStarter {
	/** Fields of the credentials file. */
	enum JsonFieldNames implements CcpJsonFieldName{
		/** The GCP project. */
		project_id
	}

	/** The credentials file read as JSON. */
	final CcpJsonRepresentation parameters;
	
	/** The receiver of the subscription. */
	private final CcpMessageReceiver topic;
	
	/** The number of executor threads. */
	private final int threads;
	
	/** Handler of the failures. */
	private final CcpBusiness notifyError ;
	
	
	/**
	 * Reads the credentials and keeps the settings.
	 * @param notifyError handler of the failures
	 * @param topic the receiver of the subscription
	 * @param threads the number of executor threads
	 */
	public CcpPubSubStarter(CcpBusiness notifyError, CcpMessageReceiver topic, int threads) {
		this.parameters = this.loadCredentials();
		this.notifyError = notifyError;
		this.threads = threads;
		this.topic = topic;
	}

	/**
	 * Reads the credentials named by {@code GOOGLE_APPLICATION_CREDENTIALS} as JSON.
	 * @return the credentials
	 */
	private CcpJsonRepresentation loadCredentials() {
		CcpStringDecorator credentialsJson = new CcpStringDecorator("GOOGLE_APPLICATION_CREDENTIALS");
		CcpPropertiesDecorator propertiesDecorator = credentialsJson.propertiesFrom();
		CcpJsonRepresentation credentialsProperties = propertiesDecorator.environmentVariablesOrClassLoaderOrFile();
		return credentialsProperties;
	}
		
	/**
	 * Subscribes the receiver to its subscription and blocks until the subscriber terminates. A missing topic or any other
	 * failure is handed to the error handler (twice: over the error and over the handler's own result).
	 * @return this starter
	 */
	public CcpPubSubStarter synchronizeMessages() {
		
		Subscriber subscriber = null;
		try {
			String projectName = this.parameters.getAsString(JsonFieldNames.project_id);
			
			ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectName, this.topic.name);
			InstantiatingExecutorProvider.Builder executorProviderBuilder = InstantiatingExecutorProvider.newBuilder();
			InstantiatingExecutorProvider.Builder executorProviderBuilderWithThreads = executorProviderBuilder.setExecutorThreadCount(this.threads);
			ExecutorProvider executorProvider = executorProviderBuilderWithThreads.build();

			FixedCredentialsProvider credentials = this.getCredentials();
			
			Builder subscriberBuilder = Subscriber.newBuilder(subscription, this.topic);
			Builder subscriberBuilderWithCredentials = subscriberBuilder.setCredentialsProvider(credentials);
			Builder subscriberBuilderWithExecutor = subscriberBuilderWithCredentials.setExecutorProvider(executorProvider);
			subscriber = subscriberBuilderWithExecutor.build(); 
			subscriber.startAsync();
			subscriber.awaitTerminated();
			return this;
		}catch (IllegalStateException e) {
			Throwable cause = e.getCause();
			boolean isTopicNotFound = cause instanceof com.google.api.gax.rpc.NotFoundException;
			if(isTopicNotFound) {
				CcpErrorPubSubTopicNotCreated topicNotCreatedError = new CcpErrorPubSubTopicNotCreated(this.topic.name);
				CcpJsonRepresentation json = new CcpJsonRepresentation(topicNotCreatedError);
				
				CcpJsonRepresentation errorNotificationResult = this.notifyError.execute(json);
				this.notifyError.execute(errorNotificationResult);
			}
			return this;
		} catch (Throwable e) {
			CcpJsonRepresentation json = new CcpJsonRepresentation(e);
			
			CcpJsonRepresentation errorNotificationResult = this.notifyError.execute(json);
			this.notifyError.execute(errorNotificationResult);
			return this;
		} finally {
			boolean subscriberWasCreated = subscriber != null;
			if (subscriberWasCreated) {
				subscriber.stopAsync();
			}
		}
	}

	/**
	 * Builds the credentials provider from {@code GOOGLE_APPLICATION_CREDENTIALS}.
	 * @return the credentials provider
	 * @throws CcpErrorPubSubCredentialsLoad when the credentials cannot be read
	 */
	private FixedCredentialsProvider getCredentials(){
		CcpStringDecorator credentialsVariableName = new CcpStringDecorator("GOOGLE_APPLICATION_CREDENTIALS");
		CcpInputStreamDecorator credentialsInputStreamDecorator = credentialsVariableName.inputStreamFrom();
		
		try (InputStream credentialsStream = credentialsInputStreamDecorator.fromEnvironmentVariablesOrClassLoaderOrFile(); ){
			ServiceAccountCredentials serviceAccountCredentials = ServiceAccountCredentials.fromStream(credentialsStream);
			FixedCredentialsProvider credentialsProvider = FixedCredentialsProvider.create(serviceAccountCredentials);
			return credentialsProvider;
			
		} catch (Exception e) {
			CcpErrorPubSubCredentialsLoad ccpErrorPubSubCredentialsLoad = new CcpErrorPubSubCredentialsLoad(e);
			throw ccpErrorPubSubCredentialsLoad;		}


	
	}

	

	/** Raised when the Pub/Sub credentials cannot be read. */
	@SuppressWarnings("serial")
	private static class CcpErrorPubSubCredentialsLoad extends RuntimeException {
		/**
		 * Wraps the cause.
		 * @param cause the original failure
		 */
		private CcpErrorPubSubCredentialsLoad(Throwable cause) {
			super(cause);
		}
	}

	/**
	 * Exception used to report that the subscription could not be started because the topic does not exist in PubSub yet.
	 */
	@SuppressWarnings("serial")
	public static class CcpErrorPubSubTopicNotCreated extends RuntimeException {
		/**
		 * Builds the message stating which topic is missing.
		 * @param topicName the name of the topic not created yet
		 */
		private CcpErrorPubSubTopicNotCreated(String topicName) {
			super("Topic still has not been created: " + topicName);
		}
	}
}
