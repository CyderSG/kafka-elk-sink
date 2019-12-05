/**
 * ElkSinkConnector
 * 
 * @author fernando
 */
package sg.com.cyder.kafka.log.elk.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ElkSinkConnector extends SinkConnector {

	protected static Logger logger = LogManager.getLogger(ElkSinkConnector.class);

	private Map<String, String> configProperties;

	public String version() {
		return Constants.VERSION;
	}

	@Override
	public ConfigDef config() {
		logger.debug("Specify the connector config definition");
		return ElkConnectorConfig.conf();
	}

	@Override
	public void start(Map<String, String> props) {
		logger.debug("Starting the ElkSinkConnector");
		try {
			configProperties = props;
			new ElkConnectorConfig(props);
			logger.debug("props: " + props);
		} catch (ConfigException e) {
			throw new ConnectException("Couldn't start ElkSinkConnector due to configuration error", e);
		}
	}

	@Override
	public void stop() {
		logger.debug("Stopping the ElkSinkConnector");
	}

	@Override
	public Class<? extends Task> taskClass() {
		return ElkSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		logger.debug("Configuring the ElkSinkConnector Task [maxTasks: " + maxTasks + "]");
		List<Map<String, String>> taskConfigs = new ArrayList<>();
		Map<String, String> taskProps = new HashMap<>();
		taskProps.putAll(configProperties);
		for (int i = 0; i < maxTasks; i++) {
			logger.debug("Adding config [" + i + "]");
			taskConfigs.add(taskProps);
		}
		return taskConfigs;
	}

}
