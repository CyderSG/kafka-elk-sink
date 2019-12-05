package sg.com.cyder.kafka.log.elk.sink.elkclient;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import sg.com.cyder.kafka.log.elk.sink.Constants;
import sg.com.cyder.kafka.log.elk.sink.Record;

public class ElkClientImpl implements ElkClient {

	private JestClient client;
	private static Logger log = LogManager.getLogger(ElkClientImpl.class);

	public ElkClientImpl(String url, int port) throws UnknownHostException {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(
				new HttpClientConfig.Builder(String.format("http://%s:%s", url, port)).multiThreaded(true).build());

		client = factory.getObject();
	}

	public void bulkSend(List<Record> records, String index, String type) {
		Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(index).defaultType(type);

		for (Record bulkItem : records) {
			JsonObject dataAsObject = bulkItem.getDataObject();
			boolean isInsert = bulkItem.getBehaviour().equals(Constants.insertedFlagValue);

			if (isInsert) {
				bulkBuilder.addAction(new Index.Builder(dataAsObject).build());
			}
		}

		try {
			BulkResult execute = client.execute(bulkBuilder.build());
			String errorMessage = execute.getErrorMessage();

			if (errorMessage != null) {
				log.error(errorMessage);
			}
		} catch (IOException e) {
			log.error(e.toString());
		}
	}

	public void close() throws IOException {
		client.shutdownClient();
	}
}
