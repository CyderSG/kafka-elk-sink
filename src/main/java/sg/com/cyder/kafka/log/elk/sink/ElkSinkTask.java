package sg.com.cyder.kafka.log.elk.sink;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import sg.com.cyder.kafka.log.elk.sink.elkservice.ElkService;
import sg.com.cyder.kafka.log.elk.sink.elkservice.ElkServiceImpl;

public class ElkSinkTask extends SinkTask {

	private static Logger log = LogManager.getLogger(ElkSinkTask.class);
	private ElkService elasticService;

	public String version() {
		return Constants.VERSION;
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
		log.debug("Flushing the queue");
	}

	@Override
	public void put(Collection<SinkRecord> collection) {
		try {
			log.debug("Put [" + collection + "]");
			Collection<String> recordsAsString = collection.stream().map(r -> String.valueOf(r.value()))
					.collect(Collectors.toList());
			log.debug("Records [" + recordsAsString + "]");
			elasticService.process(recordsAsString);
		} catch (Exception e) {
			log.error("Error while processing records");
			log.error(e.toString());
		}
	}

	@Override
	public void start(Map<String, String> map) {
		log.debug("Starting the task [" + map + "]");
		elasticService = new ElkServiceImpl(null, new ElkConnectorConfig(map));
	}

	@Override
	public void stop() {
		try {
			log.debug("Stopping the task");
			elasticService.closeClient();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}

}
