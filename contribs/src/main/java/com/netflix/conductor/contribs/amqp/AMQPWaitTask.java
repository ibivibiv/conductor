package com.netflix.conductor.contribs.amqp;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

@Singleton
public class AMQPWaitTask extends WorkflowSystemTask {

	static final String REQUEST_PARAMETER_NAME = "amqp_request";
	private static final String NAME = "AMQP_WAIT";
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
	AMQPWaitManager waitManager;
	private ConnectionFactory factory;
	private Connection connection;
	private AMQPWaitTask.Input input;

	private static final Logger logger = LoggerFactory.getLogger(AMQPWaitTask.class);

	@Inject
	public AMQPWaitTask(Configuration config, AMQPWaitManager clientManager) {
		super(NAME);
		this.config = config;
		this.requestParameter = REQUEST_PARAMETER_NAME;
		this.waitManager = clientManager;
		logger.info("AMQPTask initialized...");
		System.out.println("*******************************check if I can debug this way");

	}

	private static ObjectMapper objectMapper() {
		System.out.println("******************************* mapper start");
		final ObjectMapper om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		System.out.println("******************************* mapper stop");
		return om;
	}

	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) {
		task.setStatus(Status.IN_PROGRESS);
	}

	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {

		System.out.println("******************************* start wait ");

		long taskStartMillis = Instant.now().toEpochMilli();
		System.out.println("******************************* millis ");
		task.setWorkerId(config.getServerId());
		System.out.println("******************************* workerid ");
		Object request = task.getInputData().get(requestParameter);
		System.out.println("******************************* request " + request.toString());

		if (Objects.isNull(request)) {
			markTaskAsFailed(task, MISSING_REQUEST);
			System.out.println("******************************* missing request " + request);
			return false;
		}

		AMQPWaitTask.Input input = om.convertValue(request, AMQPWaitTask.Input.class);

		System.out.println("******************************* request ok");

		if (StringUtils.isBlank(input.getQueue())) {
			markTaskAsFailed(task, MISSING_AMQP_QUEUE);
			System.out.println("******************************* missing queue " + input.getQueue());
			return false;
		}

		if (StringUtils.isBlank(input.getUserName())) {
			markTaskAsFailed(task, MISSING_AMQP_USERNAME);
			System.out.println("******************************* missing username " + input.getUserName());
			return false;
		}

		if (StringUtils.isBlank(input.getPassword())) {
			markTaskAsFailed(task, MISSING_AMQP_PASSWORD);
			System.out.println("******************************* missing password " + input.getPassword());
			return false;
		}

		if (Objects.isNull(input.getValue())) {
			markTaskAsFailed(task, MISSING_AMQP_VALUE);
			System.out.println("******************************* missing input " + input.getValue());
			return false;
		}

		System.out.println("******************************* value ok");

		boolean consumed = false;

		try {

			this.factory = new ConnectionFactory();
			System.out.println("******************************* factory ");
			this.factory.setUsername(input.getUserName());
			this.factory.setPassword(input.getPassword());
			logger.info("AMQP Connection Factory initialized...");
			this.factory.setHost(input.getHosts());

			System.out.println("*******************************factory done");

			this.connection = factory.newConnection();

			com.rabbitmq.client.Channel channel = connection.createChannel();
			channel.queueDeclare(workflow.getWorkflowId() + task.getTaskDefName(), true, false, true, null);

			GetResponse response = channel.basicGet(workflow.getWorkflowId() + task.getTaskDefName(), false);
			if (response != null) {
				String message = new String(response.getBody(), "UTF-8");
				task.setStatus(Status.COMPLETED);
				System.out.println("*******************************set completed");

				consumed = true;
				channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
			}

			channel.close();
			connection.close();

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(String.format("Failed to invoke amqp task for input {} - unknown exception: {}", input), e);
			markTaskAsFailed(task, FAILED_TO_INVOKE + e.getMessage());
			consumed = true;
		}

		return consumed;
	}

	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) {
		// TODO Auto-generated method stub
		super.cancel(workflow, task, executor);
	}

	private void markTaskAsFailed(Task task, String reasonForIncompletion) {
		task.setReasonForIncompletion(reasonForIncompletion);
		task.setStatus(Task.Status.FAILED);
	}

	public static class Input {

		private Map<String, Object> headers = new HashMap<>();

		private String hosts;

		private String value;

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

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
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
