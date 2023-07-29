package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInject;
import com.ccp.jn.async.AsyncServices;
import com.ccp.jn.async.business.NotifyError;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

public class JnMessageReceiver implements MessageReceiver {
	
	@CcpDependencyInject
	private final NotifyError notifyError =  new NotifyError();

	private final String topic;
	
	public JnMessageReceiver(String topic) {
		this.topic = topic;
	}

	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		try {
			ByteString data = message.getData();
			String receivedMessage = data.toStringUtf8();
			CcpMapDecorator mdMessage = new CcpMapDecorator(receivedMessage);
			AsyncServices.executeProcess(this.topic, mdMessage);
			consumer.ack();
		} catch (Exception e) {
			this.notifyError.sendErrorToSupport(e);
			consumer.nack();
		}
		
	}

}
