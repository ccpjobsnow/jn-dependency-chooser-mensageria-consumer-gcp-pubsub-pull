package com.ccp.integrations.mensageria.consumer.gcp.pubsub;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInject;
import com.ccp.especifications.instant.messenger.CcpInstantMessenger;
import com.ccp.process.CcpProcess;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

public class JnMessageReceiver implements MessageReceiver {
	
	@CcpDependencyInject
	private final CcpProcess process;
	
	@CcpDependencyInject
	private CcpInstantMessenger instantMessenger;

	private CcpMapDecorator parameters;

	public JnMessageReceiver(CcpProcess process, CcpMapDecorator parameters) {
		this.parameters = parameters;
		this.process = process;
	}

	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		try {
			ByteString data = message.getData();
			String receivedMessage = data.toStringUtf8();
			CcpMapDecorator mdMessage = new CcpMapDecorator(receivedMessage);
			this.process.execute(mdMessage);
			consumer.ack();
		} catch (Exception e) {
			this.instantMessenger.sendErrorToSupport(this.parameters, e);
			consumer.nack();
		}
		
	}

}
