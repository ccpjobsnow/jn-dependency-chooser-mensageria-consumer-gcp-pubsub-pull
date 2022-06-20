package com.ccp.implementations.mensageria.consumer.gcp.pubsub;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.especifications.mensageria.consumer.CcpMensageriaMessageConsumer;

class MensageriaMessageConsumerGcpPubSub implements CcpMensageriaMessageConsumer {

	@Override
	public CcpMapDecorator onConsumeMessage(CcpMapDecorator message) {
		return message;
	}

}
