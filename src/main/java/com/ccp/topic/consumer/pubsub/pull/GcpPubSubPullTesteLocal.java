package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.implementations.db.bulk.elasticsearch.CcpElasticSerchDbBulk;
import com.ccp.implementations.db.dao.elasticsearch.CcpElasticSearchDao;
import com.ccp.implementations.db.utils.elasticsearch.CcpElasticSearchDbRequest;
import com.ccp.implementations.db.utils.elasticsearch.CcpElasticSearchQueryExecutor;
import com.ccp.implementations.emails.sendgrid.CcpSendGridEmailSender;
import com.ccp.implementations.file.bucket.gcp.CcpGcpFileBucket;
import com.ccp.implementations.http.apache.mime.CcpApacheMimeHttp;
import com.ccp.implementations.instant.messenger.telegram.CcpTelegramInstantMessenger;
import com.ccp.implementations.text.extractor.apache.tika.CcpGsonJsonHandler;
import com.ccp.implementations.text.extractor.apache.tika.CcpApacheTikaTextExtractor;
import com.ccp.jn.async.JnAsyncBusiness;
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
		CcpMapDecorator md = new CcpMapDecorator("{'credentials': 'credentials.json', 'project': 'jn-hmg',  'threads': '1'}");
		JnTopic[] topics = JnTopic.values();
//		for (JnTopic topic : Arrays.asList(JnTopic.sendUserToken)) {
		for (JnTopic topic : topics) {
			CcpMapDecorator put = md.put("topic", topic.name());
			new Thread(() -> new JnPubSubStarter(put, mdMessage -> JnAsyncBusiness.executeProcess(topic.name(), mdMessage)).synchronizeMessages()).start();
		}
	}
}
