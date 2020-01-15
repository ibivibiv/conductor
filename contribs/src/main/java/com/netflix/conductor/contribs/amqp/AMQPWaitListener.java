package com.netflix.conductor.contribs.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.tasks.TaskResult.Status;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.Workflow;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AMQPWaitListener {

	private static final String URL = "http://localhost:8080/api/";

	public AMQPWaitListener() {

		Thread listener = new Thread(new Listener());
		listener.start();

	}

	class Listener implements Runnable {

		@Override
		public void run() {

			try {

				String url = new String(Files.readAllBytes(Paths.get("/url.config")));

				TaskClient taskClient = new TaskClient();
				WorkflowClient workflowClient = new WorkflowClient();
				taskClient.setRootURI(URL);
				workflowClient.setRootURI(URL);

				ConnectionFactory factory = new ConnectionFactory();
				factory.setUri(url.trim());
				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();

				channel.queueDeclare("conductor", true, false, false, null);
				System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

				DeliverCallback deliverCallback = (consumerTag, delivery) -> {
					try {
						String message = new String(delivery.getBody(), "UTF-8");
						System.out.println(" [x] Received '" + message + "'");
						ObjectMapper mapper = new ObjectMapper();
						Map<String, String> map = mapper.readValue(message.trim(), Map.class);
						
						String workflowId = map.get("workflowId");
						String taskDefName = map.get("taskDefName");
						Workflow result = workflowClient.getWorkflow(workflowId, true);
						Task task = result.getTaskByRefName(taskDefName);
						TaskResult taskResult = new TaskResult();
						taskResult.setWorkflowInstanceId(task.getWorkflowInstanceId());
						taskResult.setTaskId(task.getTaskId());
						taskResult.setStatus(Status.COMPLETED);
						taskResult.setWorkerId("AMQPLISTER");
						taskClient.updateTask(taskResult);
						@SuppressWarnings("unchecked")
					    Map<String, Object> targetMap = (Map<String, Object>) ((Object) map);
						result.setInput(targetMap);
					} catch (Exception x) {
						x.printStackTrace();
						channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
						Thread listener = new Thread(new Listener());
						listener.start();

					}
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

				};
				channel.basicConsume("conductor", false, deliverCallback, consumerTag -> {
				});

			} catch (Exception e) {
				e.printStackTrace();
				Thread listener = new Thread(new Listener());
				listener.start();

			}

		}

	}

}
