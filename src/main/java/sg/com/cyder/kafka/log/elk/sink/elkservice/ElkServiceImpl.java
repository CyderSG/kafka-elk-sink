package sg.com.cyder.kafka.log.elk.sink.elkservice;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import sg.com.cyder.kafka.log.elk.sink.Constants;
import sg.com.cyder.kafka.log.elk.sink.ElkConnectorConfig;
import sg.com.cyder.kafka.log.elk.sink.Record;
import sg.com.cyder.kafka.log.elk.sink.elkclient.ElkClient;
import sg.com.cyder.kafka.log.elk.sink.elkclient.ElkClientImpl;

public class ElkServiceImpl implements ElkService {

	private Gson gson;
	private String statusFlag;
	private String indexName;
	private String typeName;
	private ElkClient elasticClient;
	private String dataListArrayName;
	private static Logger log = LogManager.getLogger(ElkServiceImpl.class);

	public ElkServiceImpl(ElkClient elasticClient, ElkConnectorConfig config) {
		statusFlag = config.getFlagField();
		indexName = config.getIndexName();
		typeName = config.getTypeName();
		dataListArrayName = config.getDataListArrayField();

		PrepareJsonConverters();

		if (elasticClient == null) {
			try {
				elasticClient = new ElkClientImpl(config.getElasticUrl(), config.getElasticPort());
			} catch (UnknownHostException e) {
				log.error("The host is unknown, exception stacktrace: " + e.toString());
			}
		}

		this.elasticClient = elasticClient;
	}
	
	public static void main(String args[]) {
		String recordStr = "{\"log\":\"2019-12-05T08:26:58.847Z [AAAAA] debug: API Reques";
//		System.out.println(recordStr.matches("/\\[.*\\]/g"));
		System.out.println(recordStr.matches(".*\\[.*\\].*"));
	}

	@Override
	public void process(Collection<String> recordsAsString) {
		List<Record> recordList = new ArrayList<>();

		recordsAsString.forEach(recordStr -> {
			try {

				log.debug("Verifying record [" + recordStr + "]");
				if (recordStr.matches(".*\\[.*\\].*")) {
					log.debug("Processing record [" + recordStr + "]");
					JsonObject recordAsJson = gson.fromJson(recordStr, JsonObject.class);
					String behavior = Constants.insertedFlagValue;
					recordList.add(new Record(recordAsJson, behavior));
				}

			} catch (JsonSyntaxException e) {
				log.error("Cannot deserialize json string, which is : " + recordStr);
			} catch (Exception e) {
				log.error("CYDER: Cannot process data, which is : " + recordStr + ", with error [" + e.getMessage()
						+ "]");
			}
		});

		try {
			elasticClient.bulkSend(recordList, indexName, typeName);
		} catch (Exception e) {
			log.error("Something failed, here is the error:");
			log.error(e.toString());
		}
	}

	@Override
	public void closeClient() throws IOException {
		elasticClient.close();
	}

	private void PrepareJsonConverters() {
		JsonConverter converter = new JsonConverter();
		converter.configure(Collections.singletonMap("schemas.enable", "false"), false);

		gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
				.setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").create();
	}
}
