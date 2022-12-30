package com.ccp.integrations.mensageria.consumer.gcp.pubsub;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpSpecification;
import com.ccp.process.CcpProcess;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

public class JnMessageReceiver implements MessageReceiver {
	
	@CcpSpecification
	private CcpProcess process;
	
	public JnMessageReceiver(CcpProcess process) {
		this.process = process;
	}



	@Override
	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		try {
			ByteString data = message.getData();
			String receivedMessage = data.toStringUtf8();
			CcpMapDecorator mdMessage = new CcpMapDecorator(receivedMessage);
			this.process.execute(mdMessage);
			consumer.ack();
		} catch (Exception e) {
			consumer.nack();
		}
		
	}

}
