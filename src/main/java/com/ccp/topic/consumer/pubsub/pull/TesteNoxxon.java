package com.ccp.topic.consumer.pubsub.pull;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.implementations.db.bulk.elasticsearch.Bulk;
import com.ccp.implementations.db.dao.elasticsearch.Dao;
import com.ccp.implementations.db.utils.elasticsearch.DbUtils;
import com.ccp.implementations.db.utils.elasticsearch.Query;
import com.ccp.implementations.emails.sendgrid.Email;
import com.ccp.implementations.file.bucket.gcp.FileBucket;
import com.ccp.implementations.http.apache.mime.Http;
import com.ccp.implementations.instant.messenger.telegram.InstantMessenger;
import com.ccp.implementations.text.extractor.apache.tika.JsonHandler;
import com.ccp.implementations.text.extractor.apache.tika.TextExtractor;
import com.ccp.jn.async.AsyncServices;

public class TesteNoxxon {

	public static void main(String[] args) {
		CcpDependencyInjection.loadAllDependencies
		(
				new Http(),
				new JsonHandler(),
				new InstantMessenger(),
				new TextExtractor(),
				new FileBucket(),
				new DbUtils(),
				new Email(),
				new Query(),
				new Bulk(),
				new Dao()
		);
		CcpMapDecorator md = new CcpMapDecorator("{'topic':'bigquery','credentials': 'spt.json', 'project': 'noxxonsat-nxnet-spt',  'threads': '1'}");
		new Thread(() -> new PubSubStarter(md, mdMessage -> AsyncServices.execute("bigquery", mdMessage)).synchronizeMessages()).start();

	}
}
