package com.netflix.conductor.contribs.amqp;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.core.config.Configuration;

import java.util.Objects;
import java.util.Properties;

public class AMQPWaitManager {

	public static final String AMQP_WAIT_REQUEST_TIMEOUT_MS = "amqp.wait.request.timeout.ms";
	public static final String DEFAULT_REQUEST_TIMEOUT = "100";

	public final String requestTimeoutConfig;

	public AMQPWaitManager(Configuration configuration) {
		this.requestTimeoutConfig = configuration.getProperty(AMQP_WAIT_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT);
	}

	@VisibleForTesting
	Properties getProducerProperties(AMQPWaitTask.Input input) {

		Properties configProperties = new Properties();

		String requestTimeoutMs = requestTimeoutConfig;

		if (Objects.nonNull(input.getRequestTimeoutMs())) {
			requestTimeoutMs = String.valueOf(input.getRequestTimeoutMs());
		}

		return configProperties;
	}
}
