package com.ccp.especifications.mensageria.consumer.gcp.pubsub;

import java.util.function.Function;

import com.ccp.dependency.injection.CcpImplementation;
import com.ccp.especifications.mensageria.consumer.CcpMensageriaParameters;
import com.ccp.especifications.mensageria.consumer.CcpMensageriaStarter;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.Subscriber.Builder;
import com.google.pubsub.v1.ProjectSubscriptionName;

@CcpImplementation
public class CcpMensageriaStarterGcpPubSub implements CcpMensageriaStarter {

	@Override
	public void synchronize(String topicName, Function<String, CcpMensageriaParameters> 
	producer) {
		
		CcpMensageriaParameters parameters = producer.apply(topicName);
		String projectName = parameters.tenantName;
		int threads = parameters.threadsQuantity;
		String fila = parameters.topicName;
		ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectName, fila);
		ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder()
				.setExecutorThreadCount(threads).build();
		FixedCredentialsProvider credentials = parameters.getCredentialsFile();
		MessageReceiver topic = parameters.getMessageReceiver();		
		Builder newBuilder = Subscriber.newBuilder(subscription, topic);
		Builder setCredentialsProvider = newBuilder.setCredentialsProvider(credentials);
		Builder setExecutorProvider = setCredentialsProvider.setExecutorProvider(
				executorProvider);
		Subscriber subscriber = setExecutorProvider.build(); 
		subscriber.startAsync();
		subscriber.awaitTerminated();
	}
}
