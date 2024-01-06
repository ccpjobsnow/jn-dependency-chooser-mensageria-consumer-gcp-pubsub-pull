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
import com.ccp.implementations.text.extractor.apache.tika.CcpApacheTikaTextExtractor;
import com.jn.commons.utils.JnTopic;

public class GcpPubSubPullTesteLocal {

	
	public static void main(String[] args) {
		CcpDependencyInjection.loadAllDependencies
		(
				new CcpApacheMimeHttp(),
				new CcpGsonJsonHandler(),
				new CcpTelegramInstantMessenger(),
				new CcpApacheTikaTextExtractor(),
				new CcpGcpFileBucket(),
				new CcpElasticSearchDbRequest(),
				new CcpSendGridEmailSender(),
				new CcpElasticSearchQueryExecutor(),
				new CcpElasticSerchDbBulk(),
				new CcpElasticSearchDao()
		);
		JnTopic[] topics = JnTopic.values();
		for (JnTopic topic : topics) {
			String topicName = topic.name();
			JnMessageReceiver xxx = new JnMessageReceiver(topicName);
			new Thread(() -> new JnPubSubStarter(xxx, 1).synchronizeMessages()).start();
		}
	}
}
