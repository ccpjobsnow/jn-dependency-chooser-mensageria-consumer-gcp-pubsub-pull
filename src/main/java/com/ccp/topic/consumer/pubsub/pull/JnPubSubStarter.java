package com.ccp.topic.consumer.pubsub.pull;

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
import com.jn.commons.entities.JnEntityAsyncTask;
public class JnPubSubStarter { 

	public static void main(String[] args) {
		
		CcpDependencyInjection.loadAllDependencies
		(
				new CcpTelegramInstantMessenger(),
				new CcpGsonJsonHandler(),
				new CcpGcpFileBucket(),
				new CcpElasticSearchDbRequest(),
				new CcpSendGridEmailSender(),
				new CcpElasticSearchQueryExecutor(),
				new CcpApacheMimeHttp(),
				new CcpElasticSerchDbBulk(),
				new CcpElasticSearchDao()
		);
		String topicName = args[0];
		
		JnAsyncBusinessNotifyError notifyError = new JnAsyncBusinessNotifyError();
		CcpMessageReceiver topic = new CcpMessageReceiver(notifyError, new JnEntityAsyncTask(), topicName);
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
