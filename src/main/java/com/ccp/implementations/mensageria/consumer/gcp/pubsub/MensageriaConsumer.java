package com.ccp.implementations.mensageria.consumer.gcp.pubsub;

import com.ccp.dependency.injection.CcpModuleExporter;

public class MensageriaConsumer implements CcpModuleExporter {

	@Override
	public Object export() {
		return new MensageriaMessageConsumerGcpPubSub();
	}
	
}
