package com.ccp.jn.topic.consumer.pubsub.pull;

import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.implementations.db.bulk.elasticsearch.CcpElasticSerchDbBulk;
import com.ccp.implementations.db.dao.elasticsearch.CcpElasticSearchDao;
import com.ccp.implementations.db.query.elasticsearch.CcpElasticSearchQueryExecutor;
import com.ccp.implementations.db.utils.elasticsearch.CcpElasticSearchDbRequest;
import com.ccp.implementations.email.sendgrid.CcpSendGridEmailSender;
import com.ccp.implementations.file.bucket.gcp.CcpGcpFileBucket;
import com.ccp.implementations.http.apache.mime.CcpApacheMimeHttp;
import com.ccp.implementations.instant.messenger.telegram.CcpTelegramInstantMessenger;
import com.ccp.implementations.json.gson.CcpGsonJsonHandler;
import com.ccp.jn.async.business.JnAsyncBusinessNotifyError;
import com.ccp.jn.async.business.factory.CcpJnAsyncBusinessFactory;
import com.ccp.topic.consumer.pubsub.pull.CcpMessageReceiver;
import com.ccp.topic.consumer.pubsub.pull.CcpPubSubStarter;
import com.jn.commons.entities.JnEntityAsyncTask;
public class JnPubSubStarter { 

	public static void main(String[] args) {
		
		CcpDependencyInjection.loadAllDependencies
		(
				new CcpElasticSearchQueryExecutor(),
				new CcpTelegramInstantMessenger(),
				new CcpElasticSearchDbRequest(),
				new CcpJnAsyncBusinessFactory(),
				new CcpSendGridEmailSender(),
				new CcpElasticSerchDbBulk(),
				new CcpElasticSearchDao(),
				new CcpGsonJsonHandler(),
				new CcpApacheMimeHttp(),
				new CcpGcpFileBucket()
		);
		
		String topicName = args[0];
		
		JnAsyncBusinessNotifyError notifyError = new JnAsyncBusinessNotifyError();
		JnAsyncBusinessNotifyError jnAsyncBusinessNotifyError = new JnAsyncBusinessNotifyError();

		CcpMessageReceiver topic = new CcpMessageReceiver(notifyError, JnEntityAsyncTask.INSTANCE, topicName, jnAsyncBusinessNotifyError);
		int threads = getThreads(args);
		CcpPubSubStarter pubSubStarter = new CcpPubSubStarter(notifyError, topic, threads);
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
