package sg.com.cyder.kafka.log.elk.sink.elkservice;

import java.io.IOException;
import java.util.Collection;

public interface ElkService {

	void process(Collection<String> recordsAsString);

	void closeClient() throws IOException;
}
