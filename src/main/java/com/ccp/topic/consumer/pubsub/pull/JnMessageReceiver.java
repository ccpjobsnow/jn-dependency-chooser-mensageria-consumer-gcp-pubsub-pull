package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.jn.async.AsyncServices;
import com.ccp.jn.async.business.NotifyError;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

public class JnMessageReceiver implements MessageReceiver {
	
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
			try {
				AsyncServices.executeProcess(this.topic, mdMessage);
			} catch (Throwable e) {
				CcpMapDecorator put = new CcpMapDecorator().put("topic", this.topic).put("values", mdMessage);
				throw new RuntimeException(put.asPrettyJson(), e);
			}
			consumer.ack();
		} catch (Throwable e) {
			this.notifyError.apply(e);
			consumer.nack();
		}
		
	}

}
