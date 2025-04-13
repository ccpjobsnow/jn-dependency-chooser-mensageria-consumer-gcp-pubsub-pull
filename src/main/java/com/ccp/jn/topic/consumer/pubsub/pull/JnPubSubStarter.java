package com.ccp.jn.topic.consumer.pubsub.pull;

import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.implementations.db.bulk.elasticsearch.CcpElasticSerchDbBulk;
import com.ccp.implementations.db.crud.elasticsearch.CcpElasticSearchCrud;
import com.ccp.implementations.db.query.elasticsearch.CcpElasticSearchQueryExecutor;
import com.ccp.implementations.db.utils.elasticsearch.CcpElasticSearchDbRequest;
import com.ccp.implementations.email.sendgrid.CcpSendGridEmailSender;
import com.ccp.implementations.file.bucket.gcp.CcpGcpFileBucket;
import com.ccp.implementations.http.apache.mime.CcpApacheMimeHttp;
import com.ccp.implementations.instant.messenger.telegram.CcpTelegramInstantMessenger;
import com.ccp.implementations.json.gson.CcpGsonJsonHandler;
import com.ccp.topic.consumer.pubsub.pull.CcpMessageReceiver;
import com.ccp.topic.consumer.pubsub.pull.CcpPubSubStarter;
import com.jn.business.JnBusinessNotifyError;
import com.jn.entities.JnEntityAsyncTask;
public class JnPubSubStarter { 

	public static void main(String[] args) {
		
		CcpDependencyInjection.loadAllDependencies
		(
				new CcpElasticSearchQueryExecutor(),
				new CcpTelegramInstantMessenger(),
				new CcpElasticSearchDbRequest(),
				new CcpSendGridEmailSender(),
				new CcpElasticSerchDbBulk(),
				new CcpElasticSearchCrud(),
				new CcpGsonJsonHandler(),
				new CcpApacheMimeHttp(),
				
				new CcpGcpFileBucket()
		);
		
		String topicName = args[0];
		
		CcpMessageReceiver topic = new CcpMessageReceiver(JnBusinessNotifyError.INSTANCE, JnEntityAsyncTask.ENTITY, topicName, JnBusinessNotifyError.INSTANCE);
		int threads = getThreads(args);
		CcpPubSubStarter pubSubStarter = new CcpPubSubStarter(JnBusinessNotifyError.INSTANCE, topic, threads);
		pubSubStarter.synchronizeMessages();
	}
	private static int getThreads(String[] args) {
		try {
			String s = args[1];
			Integer valueOf = Integer.valueOf(s);
			return valueOf;
		} catch (Exception e) {
			return 1;
		}
	}

}
