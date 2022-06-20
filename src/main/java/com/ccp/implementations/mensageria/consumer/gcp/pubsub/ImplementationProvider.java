package com.ccp.implementations.mensageria.consumer.gcp.pubsub;

import com.ccp.dependency.injection.CcpEspecification.DefaultImplementationProvider;

public class ImplementationProvider extends DefaultImplementationProvider {

	@Override
	public Object getImplementation() {
		return new MensageriaMessageConsumerGcpPubSub();
	}
	
}
