package com.ccp.topic.consumer.pubsub.pull;

import java.util.function.Function;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.jn.async.business.JnAsyncBusinessNotifyError;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

public class JnMessageReceiver implements MessageReceiver {
	
	private final JnAsyncBusinessNotifyError notifyError =  new JnAsyncBusinessNotifyError();

	private final String topic;

	private final Function<CcpJsonRepresentation, CcpJsonRepresentation> function;
	
	public JnMessageReceiver(String topic, Function<CcpJsonRepresentation, CcpJsonRepresentation> function) {
		this.function = function;
		this.topic = topic;
	}

	public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
		try {
			ByteString data = message.getData();
			String receivedMessage = data.toStringUtf8();
			CcpJsonRepresentation mdMessage = new CcpJsonRepresentation(receivedMessage);
			try {
				this.function.apply(mdMessage);
			} catch (Throwable e) {
				CcpJsonRepresentation put = CcpConstants.EMPTY_JSON.put("topic", this.topic).put("values", mdMessage);
				throw new RuntimeException(put.asPrettyJson(), e);
			}
			consumer.ack();
		} catch (Throwable e) {
			this.notifyError.apply(e);
			consumer.nack();
		}
		
	}

}
