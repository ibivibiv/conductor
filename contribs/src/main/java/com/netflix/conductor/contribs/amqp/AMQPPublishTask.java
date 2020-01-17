package com.netflix.conductor.contribs.amqp;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.rabbitmq.client.AMQP.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Singleton
public class AMQPPublishTask extends WorkflowSystemTask {

	static final String REQUEST_PARAMETER_NAME = "amqp_request";
	private static final String NAME = "AMQP_PUBLISH";
	private static final String MISSING_REQUEST = "Missing AMQP request. Task input MUST have a '"
			+ REQUEST_PARAMETER_NAME
			+ "' key with AMQPTask.Input as value. See documentation for AMQPTask for required input parameters";
	private static final String MISSING_HOST_SERVERS = "No boot strap servers specified";
	private static final String MISSING_AMQP_QUEUE = "Missing AMQP topic. See documentation for AMQPTask for required input parameters";
	private static final String MISSING_AMQP_VALUE = "Missing AMQP value.  See documentation for AMQPTask for required input parameters";
	private static final String MISSING_AMQP_USERNAME = "Missing AMQP username.  See documentation for AMQPTask for required input parameters";
	private static final String MISSING_AMQP_PASSWORD = "Missing AMQP password.  See documentation for AMQPTask for required input parameters";
	private static final String FAILED_TO_INVOKE = "Failed to invoke AMQP task due to: ";

	private ObjectMapper om = objectMapper();
	private Configuration config;
	private String requestParameter;
	AMQPProducerManager producerManager;
	private ConnectionFactory factory;
	private Connection connection;
	private AMQPPublishTask.Input input;

	private static final Logger logger = LoggerFactory.getLogger(AMQPPublishTask.class);

	@Inject
	public AMQPPublishTask(Configuration config, AMQPProducerManager clientManager) {
		super(NAME);
		this.config = config;
		this.requestParameter = REQUEST_PARAMETER_NAME;
		this.producerManager = clientManager;
		logger.info("AMQPTask initialized...");
		

	}

	private static ObjectMapper objectMapper() {
		
		final ObjectMapper om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		
		return om;
	}

	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) {

		

		long taskStartMillis = Instant.now().toEpochMilli();
		
		task.setWorkerId(config.getServerId());
		
		Object request = task.getInputData().get(requestParameter);
		

		if (Objects.isNull(request)) {
			markTaskAsFailed(task, MISSING_REQUEST);
			
			return;
		}

		AMQPPublishTask.Input input = om.convertValue(request, AMQPPublishTask.Input.class);

		

		if (StringUtils.isBlank(input.getQueue())) {
			markTaskAsFailed(task, MISSING_AMQP_QUEUE);
			return;
		}

		if (StringUtils.isBlank(input.getUserName())) {
			markTaskAsFailed(task, MISSING_AMQP_USERNAME);
			return;
		}

		if (StringUtils.isBlank(input.getPassword())) {
			markTaskAsFailed(task, MISSING_AMQP_PASSWORD);
			return;
		}

		if (Objects.isNull(input.getValue())) {
			markTaskAsFailed(task, MISSING_AMQP_VALUE);
			return;
		}

		
		try {
			this.factory = new ConnectionFactory();
//			this.factory.setUsername(input.getUserName());
//			this.factory.setPassword(input.getPassword());
//			logger.info("AMQP Connection Factory initialized...");
//			this.factory.setHost(input.getHosts());

			factory.setUri(
					"amqps://"+input.getUserName()+":"+input.getPassword()+"@"+input.getHosts()+":30910");

			this.connection = factory.newConnection();

			
			com.rabbitmq.client.Channel channel = connection.createChannel();
			channel.queueDeclare(input.getQueue(), true, false, false, null);
			
			channel.basicPublish("", input.getQueue(), MessageProperties.PERSISTENT_TEXT_PLAIN,
					input.getValue().toString().getBytes());
			task.setStatus(Task.Status.COMPLETED);
			long timeTakenToCompleteTask = Instant.now().toEpochMilli() - taskStartMillis;
			logger.info("Published message {}, Time taken {}", input, timeTakenToCompleteTask);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(String.format("Failed to invoke amqp task for input {} - unknown exception: {}", input), e);
			markTaskAsFailed(task, FAILED_TO_INVOKE + e.getMessage());
		}
	}

	private void markTaskAsFailed(Task task, String reasonForIncompletion) {
		task.setReasonForIncompletion(reasonForIncompletion);
		task.setStatus(Task.Status.FAILED);
	}

	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {
		return false;
	}

	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) {
		task.setStatus(Task.Status.CANCELED);
	}

	@Override
	public boolean isAsync() {
		return false;
	}

	public static class Input {

		private Map<String, Object> headers = new HashMap<>();

		private String hosts;

		private Object value;

		private Integer requestTimeoutMs;

		private String queue;

		private String userName;

		private String password;

		public Map<String, Object> getHeaders() {
			return headers;
		}

		public void setHeaders(Map<String, Object> headers) {
			this.headers = headers;
		}

		public String getHosts() {
			return hosts;
		}

		public void setHosts(String hosts) {
			this.hosts = hosts;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public Integer getRequestTimeoutMs() {
			return requestTimeoutMs;
		}

		public void setRequestTimeoutMs(Integer requestTimeoutMs) {
			this.requestTimeoutMs = requestTimeoutMs;
		}

		public String getQueue() {
			return queue;
		}

		public void setQueue(String queue) {
			this.queue = queue;
		}

		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public String getPassword() {
			return password;
		}

		public void setPassword(String password) {
			this.password = password;
		}

		@Override
		public String toString() {
			return "";
		}
	}
}
