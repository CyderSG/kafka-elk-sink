package sg.com.cyder.kafka.log.elk.sink.elkclient;

import java.io.IOException;
import java.util.List;

import sg.com.cyder.kafka.log.elk.sink.Record;

public interface ElkClient {
	void bulkSend(List<Record> records, String index, String type) throws IOException;

	void close() throws IOException;
}
