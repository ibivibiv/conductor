package com.netflix.conductor.contribs.amqp;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.core.config.Configuration;

import java.util.Objects;
import java.util.Properties;

public class AMQPProducerManager {

	public static final String AMQP_PUBLISH_REQUEST_TIMEOUT_MS = "amqp.publish.request.timeout.ms";
	public static final String DEFAULT_REQUEST_TIMEOUT = "100";

	public final String requestTimeoutConfig;

	public AMQPProducerManager(Configuration configuration) {
		System.out.println("*************************producermanager constructor");
		this.requestTimeoutConfig = configuration.getProperty(AMQP_PUBLISH_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT);
	}

	@VisibleForTesting
	Properties getProducerProperties(AMQPPublishTask.Input input) {
		
		System.out.println("*********************start get properties from input");

		Properties configProperties = new Properties();

		String requestTimeoutMs = requestTimeoutConfig;

		if (Objects.nonNull(input.getRequestTimeoutMs())) {
			requestTimeoutMs = String.valueOf(input.getRequestTimeoutMs());
		}
		
		System.out.println("*********************stop get properties from input")

		return configProperties;
	}
}
