package com.ccp.topic.consumer.pubsub.pull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInject;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.implementations.db.bulk.elasticsearch.Bulk;
import com.ccp.implementations.db.dao.elasticsearch.Dao;
import com.ccp.implementations.db.utils.elasticsearch.DbUtils;
import com.ccp.implementations.db.utils.elasticsearch.Query;
import com.ccp.implementations.emails.sendgrid.Email;
import com.ccp.implementations.file.bucket.gcp.FileBucket;
import com.ccp.implementations.http.apache.mime.Http;
import com.ccp.implementations.instant.messenger.telegram.InstantMessenger;
import com.ccp.implementations.text.extractor.apache.tika.TextExtractor;
import com.ccp.jn.async.business.NotifyError;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.Subscriber.Builder;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.jn.commons.JnEntity;
public class PubSubStarter { 

	final CcpMapDecorator parameters;
	
	@CcpDependencyInject
	private final JnMessageReceiver queue;
	
	@CcpDependencyInject
	private NotifyError notifyError = CcpDependencyInjection.getInjected(NotifyError.class);
	
	
	public PubSubStarter( CcpMapDecorator args) {
		this.parameters = args;
		String topic = this.parameters.getAsString("topic");
		this.queue = new JnMessageReceiver(topic);

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
			this.notifyError.apply(e);
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
			this.notifyError.apply(e);
			throw new RuntimeException(e);
		}
	}

	
	public static void main(String[] args) {
		CcpDependencyInjection.loadAllImplementationsProviders
		(
				new InstantMessenger(),
				new TextExtractor(),
				new FileBucket(),
				new DbUtils(),
				new Email(),
				new Query(),
				new Http(),
				new Bulk(),
				new Dao()
		);
		JnEntity.loadEntitiesMetadata();
		String json = args[0];
		CcpMapDecorator md = new CcpMapDecorator(json);
		PubSubStarter pubSubStarter = new PubSubStarter(md);
		CcpDependencyInjection.injectDependencies(pubSubStarter);
		pubSubStarter.synchronizeMessages();
		
	}
	
}
