package com.ccp.integrations.mensageria.consumer.gcp.pubsub;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInject;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.instant.messenger.CcpInstantMessenger;
import com.ccp.implementations.instant.messages.telegram.InstantMessenger;
import com.ccp.jn.sync.AsyncServices;
import com.ccp.process.CcpProcess;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.Subscriber.Builder;
import com.google.pubsub.v1.ProjectSubscriptionName;

public class PubSubStarter {

	final CcpMapDecorator parameters;
	
	@CcpDependencyInject
	CcpInstantMessenger instantMessenger;
	
	@CcpDependencyInject
	private final MessageReceiver queue;

	public PubSubStarter( CcpMapDecorator args) {
		this.parameters = args;
		String topic = this.parameters.getAsString("topic");
		CcpProcess process = AsyncServices.catalog.getAsObject(topic);
		this.queue = new JnMessageReceiver(process, this.parameters);

	}
		
	public void synchronizeMessages() {
		
		Subscriber subscriber = null;
		try {
			String topic = this.parameters.getAsString("topic");
			String projectName = this.parameters.getAsString("project");
			Integer threads = this.parameters.getAsIntegerNumber("threads");
			
			ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectName, topic);
			ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(threads).build();

			FixedCredentialsProvider credentials = this.getCredentials();
			
			Builder newBuilder = Subscriber.newBuilder(subscription, this.queue);
			Builder setCredentialsProvider = newBuilder.setCredentialsProvider(credentials);
			Builder setExecutorProvider = setCredentialsProvider.setExecutorProvider(executorProvider);
			subscriber = setExecutorProvider.build(); 
			subscriber.startAsync();
			subscriber.awaitTerminated();
		} catch (Throwable e) {
			this.instantMessenger.sendErrorToSupport(this.parameters, e);
		} finally {
			if (subscriber != null) {
				subscriber.stopAsync();
			}
		}
	}

	private FixedCredentialsProvider getCredentials(){
		
		String fileName = this.parameters.getAsString("credentials");
		File file = new File(fileName);
		
		try(InputStream credentials = new FileInputStream(file)){
			FixedCredentialsProvider create = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(credentials));
			return create;
			
		}catch(IOException e) {
			this.instantMessenger.sendErrorToSupport(this.parameters, e);
			throw new RuntimeException(e);
		}
	}

	
	public static void main(String[] args) {
		CcpDependencyInjection.loadAllImplementationsProviders(new InstantMessenger());
		String json = args[0];
		CcpMapDecorator md = new CcpMapDecorator(json);
		PubSubStarter instance = new PubSubStarter(md);
		CcpDependencyInjection.injectDependencies(instance);
	}
	
}
