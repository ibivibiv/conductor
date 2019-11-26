package com.netflix.conductor.contribs.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.List;

import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.client.http.WorkflowClient;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.tasks.TaskResult.Status;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;

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

				TaskClient taskClient = new TaskClient();
				WorkflowClient workflowClient = new WorkflowClient();
				taskClient.setRootURI(URL);
				workflowClient.setRootURI(URL);

				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost("rabbitmq-headless");
				factory.setUsername("conductor");
				factory.setPassword("conductor");
				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();

				channel.queueDeclare("conductor", true, false, false, null);
				System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

				DeliverCallback deliverCallback = (consumerTag, delivery) -> {
					try {
						String message = new String(delivery.getBody(), "UTF-8");
						System.out.println(" [x] Received '" + message + "'");
						String[] split = message.split(",");
						String workflowId = split[1];
						String taskDefName = split[0];
						String taskId = "";
						SearchResult<TaskSummary> result = taskClient.search("");
						List<TaskSummary> tasks = result.getResults();
						for (TaskSummary t : tasks) {
							taskId = t.getTaskId();
							break;
						}
						Task task = taskClient.getPendingTaskForWorkflow(split[1], split[0]);
						TaskResult taskResult = new TaskResult();
						taskResult.setTaskId(task.getTaskId());
						taskResult.setStatus(Status.COMPLETED);
						taskResult.setWorkerId("AMQPLISTER");
						taskClient.updateTask(taskResult);
					} catch (Exception x) {
						x.printStackTrace();
						channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
					}
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

				};
				channel.basicConsume("conductor", false, deliverCallback, consumerTag -> {
				});

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

}
