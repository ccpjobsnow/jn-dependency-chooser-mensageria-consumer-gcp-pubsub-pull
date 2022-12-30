package com.ccp.integrations.mensageria.consumer.gcp.pubsub;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.dependency.injection.CcpSpecification;
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
	final CcpMapDecorator args;
	@CcpSpecification
	CcpInstantMessenger instantMessenger;
	private final MessageReceiver queue;
	public PubSubStarter( CcpMapDecorator args) {
		this.args = args;
		String topic = this.args.getAsString("topic");
		CcpProcess process = AsyncServices.catalog.getAsObject(topic);
		this.queue = new JnMessageReceiver(process);

	}
		
	public void synchronizeMessages() {
		
		Subscriber subscriber = null;
		try {
			String topic = this.args.getAsString("topic");
			String projectName = this.args.getAsString("project");
			Integer threads = this.args.getAsIntegerNumber("threads");
			
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
			this.notificarErro(e);
		} finally {
			if (subscriber != null) {
				subscriber.stopAsync();
			}
		}
	}

	private void notificarErro(Throwable e) {
		String supportId = this.args.getAsString("supportId");
		String asJson = new CcpMapDecorator(e).asJson();
		this.instantMessenger.sendMessageToSupport(supportId, asJson);
	}

	private FixedCredentialsProvider getCredentials(){
		
		String fileName = this.args.getAsString("credentials");
		File file = new File(fileName);
		
		try(InputStream credentials = new FileInputStream(file)){
			FixedCredentialsProvider create = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(credentials));
			return create;
			
		}catch(IOException e) {
			this.notificarErro(e);
			throw new RuntimeException(e);
		}
	}

	
	public static void main(String[] args) {
		CcpDependencyInjection.loadAllInstances(new InstantMessenger());
		String json = args[0];
		CcpMapDecorator md = new CcpMapDecorator(json);
		PubSubStarter instance = new PubSubStarter(md);
		CcpDependencyInjection.injectDependencies(instance);

	}
	
}
