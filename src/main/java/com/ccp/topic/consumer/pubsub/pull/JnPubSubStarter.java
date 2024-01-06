package com.ccp.topic.consumer.pubsub.pull;

import java.io.IOException;
import java.io.InputStream;

import com.ccp.decorators.CcpInputStreamDecorator;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpPropertiesDecorator;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.implementations.db.bulk.elasticsearch.CcpElasticSerchDbBulk;
import com.ccp.implementations.db.dao.elasticsearch.CcpElasticSearchDao;
import com.ccp.implementations.db.query.elasticsearch.CcpElasticSearchQueryExecutor;
import com.ccp.implementations.db.utils.elasticsearch.CcpElasticSearchDbRequest;
import com.ccp.implementations.email.sendgrid.CcpSendGridEmailSender;
import com.ccp.implementations.file.bucket.gcp.CcpGcpFileBucket;
import com.ccp.implementations.http.apache.mime.CcpApacheMimeHttp;
import com.ccp.implementations.instant.messenger.telegram.CcpTelegramInstantMessenger;
import com.ccp.implementations.json.gson.CcpGsonJsonHandler;
import com.ccp.implementations.text.extractor.apache.tika.CcpApacheTikaTextExtractor;
import com.ccp.jn.async.business.JnAsyncBusinessNotifyError;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.Subscriber.Builder;
import com.google.pubsub.v1.ProjectSubscriptionName;
public class JnPubSubStarter { 

	final CcpJsonRepresentation parameters;
	
	private final JnMessageReceiver topic;
	
	private final int threads;
	
	private JnAsyncBusinessNotifyError notifyError = new JnAsyncBusinessNotifyError();
	
	
	public JnPubSubStarter(JnMessageReceiver topic, int threads) {
		this.parameters = this.loadCredentials();
		this.threads = threads;
		this.topic = topic;
	}

	private CcpJsonRepresentation loadCredentials() {
		CcpStringDecorator credentialsJson = new CcpStringDecorator("GOOGLE_APPLICATION_CREDENTIALS");
		CcpPropertiesDecorator propertiesFrom = credentialsJson.propertiesFrom();
		CcpJsonRepresentation environmentVariablesOrClassLoaderOrFile = propertiesFrom.environmentVariablesOrClassLoaderOrFile();
		return environmentVariablesOrClassLoaderOrFile;
	}
		
	public void synchronizeMessages() {
		
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
		}catch (java.lang.IllegalStateException e) {
			if(e.getCause() instanceof com.google.api.gax.rpc.NotFoundException) {
				this.notifyError.apply(new RuntimeException("Topic still has not been created: " + this.topic.name));
			}
		} catch (Throwable e) {
			this.notifyError.apply(e);
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

	
	public static void main(String[] args) {
		
		CcpDependencyInjection.loadAllDependencies
		(
				new CcpTelegramInstantMessenger(),
				new CcpGsonJsonHandler(),
				new CcpApacheTikaTextExtractor(),
				new CcpGcpFileBucket(),
				new CcpElasticSearchDbRequest(),
				new CcpSendGridEmailSender(),
				new CcpElasticSearchQueryExecutor(),
				new CcpApacheMimeHttp(),
				new CcpElasticSerchDbBulk(),
				new CcpElasticSearchDao()
		);
		String topicName = args[0];
		int threads = getThreads(args);
		JnMessageReceiver topic = new JnMessageReceiver(topicName);
		JnPubSubStarter pubSubStarter = new JnPubSubStarter(topic, threads);
		pubSubStarter.synchronizeMessages();
		
	}

	private static int getThreads(String[] args) {
		try {
			String s = args[1];
			Integer valueOf = Integer.valueOf(s);
			return valueOf;
		} catch (Exception e) {
			return 1;
		}
	}
	
}
