package com.netflix.conductor.core.execution.mapper;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ParametersUtils;
import com.netflix.conductor.core.execution.TerminateWorkflowException;
import com.netflix.conductor.dao.MetadataDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class AMQPPublishTaskMapper implements TaskMapper  {
	
	System.out.println("*********************start task mapper");
	public static final Logger logger = LoggerFactory.getLogger(AMQPPublishTaskMapper.class);

	private final ParametersUtils parametersUtils;
	private final MetadataDAO metadataDAO;

	public AMQPPublishTaskMapper(ParametersUtils parametersUtils, MetadataDAO metadataDAO) {
		this.parametersUtils = parametersUtils;
		this.metadataDAO = metadataDAO;
	}

	/**
	 * This method maps a {@link WorkflowTask} of type {@link TaskTyp#AMQP_PUBLISH}
	 * to a {@link Task} in a {@link Task.Status#SCHEDULED} state
	 *
	 * @param taskMapperContext: A wrapper class containing the {@link WorkflowTask}, {@link WorkflowDef}, {@link Workflow} and a string representation of the TaskId
	 * @return a List with just one AMQP task
	 * @throws TerminateWorkflowException In case if the task definition does not exist
	 */
	@Override
	public List<Task> getMappedTasks(TaskMapperContext taskMapperContext) throws TerminateWorkflowException {

		logger.debug("TaskMapperContext {} in AMQPPublishTaskMapper", taskMapperContext);

		WorkflowTask taskToSchedule = taskMapperContext.getTaskToSchedule();
		Workflow workflowInstance = taskMapperContext.getWorkflowInstance();
		String taskId = taskMapperContext.getTaskId();
		int retryCount = taskMapperContext.getRetryCount();

		TaskDef taskDefinition = Optional.ofNullable(taskMapperContext.getTaskDefinition())
						.orElseGet(() -> Optional.ofNullable(metadataDAO.getTaskDef(taskToSchedule.getName()))
						.orElse(null));

		Map<String, Object> input = parametersUtils.getTaskInputV2(taskToSchedule.getInputParameters(), workflowInstance, taskId, taskDefinition);

		Task amqpPublishTask = new Task();
		amqpPublishTask.setTaskType(taskToSchedule.getType());
		amqpPublishTask.setTaskDefName(taskToSchedule.getName());
		amqpPublishTask.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		amqpPublishTask.setWorkflowInstanceId(workflowInstance.getWorkflowId());
		amqpPublishTask.setWorkflowType(workflowInstance.getWorkflowName());
		amqpPublishTask.setCorrelationId(workflowInstance.getCorrelationId());
		amqpPublishTask.setScheduledTime(System.currentTimeMillis());
		amqpPublishTask.setTaskId(taskId);
		amqpPublishTask.setInputData(input);
		amqpPublishTask.setStatus(Task.Status.SCHEDULED);
		amqpPublishTask.setRetryCount(retryCount);
		amqpPublishTask.setCallbackAfterSeconds(taskToSchedule.getStartDelay());
		amqpPublishTask.setWorkflowTask(taskToSchedule);
		amqpPublishTask.setWorkflowPriority(workflowInstance.getPriority());
		if (Objects.nonNull(taskDefinition)) {
			amqpPublishTask.setRateLimitPerFrequency(taskDefinition.getRateLimitPerFrequency());
			amqpPublishTask.setRateLimitFrequencyInSeconds(taskDefinition.getRateLimitFrequencyInSeconds());
		}
		
		System.out.println("*********************stop task mapper");
		
		return Collections.singletonList(amqpPublishTask);
	}
}
