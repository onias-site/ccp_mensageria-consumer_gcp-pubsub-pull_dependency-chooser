package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.business.CcpBusiness;
import com.ccp.decorators.CcpJsonRepresentation;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.Subscriber.Builder;
import com.google.pubsub.v1.ProjectSubscriptionName;
/**
 * GCP Pub/Sub pull subscriber starter. Uses the application default credentials (the service account of the VM, or the
 * file named by {@code GOOGLE_APPLICATION_CREDENTIALS} on a developer machine), creates the {@code Subscriber} with the
 * configured number of threads and waits for messages in {@code synchronizeMessages()}.
 */
public class CcpPubSubStarter {
	/** The receiver of the subscription. */
	private final CcpMessageReceiver topic;
	
	/** The number of executor threads. */
	private final int threads;
	
	/** Handler of the failures. */
	private final CcpBusiness notifyError ;
	
	
	/**
	 * Keeps the settings.
	 * @param notifyError handler of the failures
	 * @param topic the receiver of the subscription
	 * @param threads the number of executor threads
	 */
	public CcpPubSubStarter(CcpBusiness notifyError, CcpMessageReceiver topic, int threads) {
		this.notifyError = notifyError;
		this.threads = threads;
		this.topic = topic;
	}

	/**
	 * Subscribes the receiver to its subscription and blocks until the subscriber terminates. A missing topic or any other
	 * failure is handed once to the error handler (until 2026-10-06 it ran twice, the second time over its own result, and
	 * an {@code IllegalStateException} other than the missing topic was swallowed).
	 * @return this starter
	 */
	public CcpPubSubStarter synchronizeMessages() {
		
		Subscriber subscriber = null;
		try {
			String projectName = ServiceOptions.getDefaultProjectId();
			
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
				this.notifyError.execute(json);
				return this;
			}
			CcpJsonRepresentation json = new CcpJsonRepresentation(e);
			this.notifyError.execute(json);
			return this;
		} catch (Throwable e) {
			CcpJsonRepresentation json = new CcpJsonRepresentation(e);
			this.notifyError.execute(json);
			return this;
		} finally {
			boolean subscriberWasCreated = subscriber != null;
			if (subscriberWasCreated) {
				subscriber.stopAsync();
			}
		}
	}

	/**
	 * Builds the credentials provider from the application default credentials.
	 * @return the credentials provider
	 * @throws CcpErrorPubSubCredentialsLoad when the credentials cannot be read
	 */
	private FixedCredentialsProvider getCredentials(){
		try {
			GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
			FixedCredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
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
