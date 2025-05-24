package com.ccp.topic.consumer.pubsub.pull;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

import com.ccp.decorators.CcpInputStreamDecorator;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.Subscriber.Builder;
import com.google.pubsub.v1.ProjectSubscriptionName;
public class CcpPubSubStarter { 

	final CcpJsonRepresentation parameters;
	
	private final CcpMessageReceiver topic;
	
	private final int threads;
	
	private final Function<CcpJsonRepresentation, CcpJsonRepresentation> notifyError ;
	
	
	public CcpPubSubStarter(Function<CcpJsonRepresentation, CcpJsonRepresentation> notifyError, CcpMessageReceiver topic, int threads) {
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
			String projectName = this.parameters.getAsString("project_id");
			
			ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectName, this.topic.name);
			ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(this.threads).build();

			FixedCredentialsProvider credentials = this.getCredentials();
			
			Builder newBuilder = Subscriber.newBuilder(subscription, this.topic);
			Builder setCredentialsProvider = newBuilder.setCredentialsProvider(credentials);
			Builder setExecutorProvider = setCredentialsProvider.setExecutorProvider(executorProvider);
			subscriber = setExecutorProvider.build(); 
			subscriber.startAsync();
			subscriber.awaitTerminated();
			return this;
		}catch (IllegalStateException e) {
			if(e.getCause() instanceof com.google.api.gax.rpc.NotFoundException) {
				RuntimeException ex = new RuntimeException("Topic still has not been created: " + this.topic.name);
				CcpJsonRepresentation json = new CcpJsonRepresentation(ex);
				
				CcpJsonRepresentation execute = this.notifyError.apply(json);
				this.notifyError.apply(execute);
			}
			return this;
		} catch (Throwable e) {
			CcpJsonRepresentation json = new CcpJsonRepresentation(e);
			
			CcpJsonRepresentation execute = this.notifyError.apply(json);
			this.notifyError.apply(execute);
			return this;
		} finally {
			if (subscriber != null) {
				subscriber.stopAsync();
			}
		}
	}

	private FixedCredentialsProvider getCredentials(){
		
		CcpStringDecorator ccpStringDecorator = new CcpStringDecorator("GOOGLE_APPLICATION_CREDENTIALS");
		CcpInputStreamDecorator inputStreamFrom = ccpStringDecorator.inputStreamFrom();
		InputStream is = inputStreamFrom.fromEnvironmentVariablesOrClassLoaderOrFile();

		FixedCredentialsProvider create;
		try {
			ServiceAccountCredentials fromStream = ServiceAccountCredentials.fromStream(is);
			create = FixedCredentialsProvider.create(fromStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return create;

	
	}

	
}
