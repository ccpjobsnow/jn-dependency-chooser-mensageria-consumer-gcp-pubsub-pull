package com.ccp.implementations.mensageria.consumer.gcp.pubsub;

import java.io.IOException;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpImplementation;
import com.ccp.especifications.mensageria.consumer.CcpMensageriaConsumerAck;
import com.ccp.especifications.mensageria.consumer.CcpMensageriaConsumerMessage;
import com.ccp.especifications.mensageria.consumer.CcpMensageriaParameters;
import com.ccp.especifications.mensageria.consumer.CcpMensageriaStarter;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.Subscriber.Builder;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

@CcpImplementation
public class CcpMensageriaGcpPubSubPull implements CcpMensageriaStarter {

	@Override
	public void synchronize(CcpMensageriaParameters parameters) {
		
		String projectName = parameters.tenantName;
		int threads = parameters.threadsQuantity;
		String fila = parameters.topicName;
		
		ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectName, fila);
		
		ExecutorProvider executorProvider = InstantiatingExecutorProvider.newBuilder()
				.setExecutorThreadCount(threads).build();
		
		FixedCredentialsProvider credentials;
		try {
			credentials = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(parameters.credentials));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		MessageReceiver topic = (message, consumer) -> parameters.messageReceiver.
				onConsumeMessage(new CcpPubsubMessage(message), new CcpAckReplyConsumer(consumer));		
		
		Builder newBuilder = Subscriber.newBuilder(subscription, topic);
		
		Builder setCredentialsProvider = newBuilder.setCredentialsProvider(credentials);
		
		Builder setExecutorProvider = setCredentialsProvider.setExecutorProvider(executorProvider);
		
		Subscriber subscriber = setExecutorProvider.build(); 
		
		subscriber.startAsync();
		
		subscriber.awaitTerminated();
	}

	
	static class CcpAckReplyConsumer implements CcpMensageriaConsumerAck{
		private final AckReplyConsumer consumer;
		
		
		public CcpAckReplyConsumer(AckReplyConsumer consumer) {
			this.consumer = consumer;
		}

		@Override
		public void nack() {
			this.consumer.nack();
		}

		@Override
		public void ack() {
			this.consumer.ack();
		}
		
	}
	
	
	static class CcpPubsubMessage implements CcpMensageriaConsumerMessage{
		
		private final PubsubMessage message;
		
		public CcpPubsubMessage(PubsubMessage message) {
			this.message = message;
		}

		@Override
		public CcpMapDecorator asMap() {
			String asString = this.asString();
			return new CcpMapDecorator(asString);
		}

		@Override
		public String asString() {
			String receivedMessage = this.message.getData().toStringUtf8();
			return receivedMessage;
		}
		
	}
	
}
