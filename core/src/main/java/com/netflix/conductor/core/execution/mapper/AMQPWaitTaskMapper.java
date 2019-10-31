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

public class AMQPWaitTaskMapper implements TaskMapper {
	public static final Logger logger = LoggerFactory.getLogger(AMQPWaitTaskMapper.class);

	private final ParametersUtils parametersUtils;
	private final MetadataDAO metadataDAO;

	public AMQPWaitTaskMapper(ParametersUtils parametersUtils, MetadataDAO metadataDAO) {
		this.parametersUtils = parametersUtils;
		this.metadataDAO = metadataDAO;
	}

	/**
	 * This method maps a {@link WorkflowTask} of type {@link TaskTyp#AMQP_PUBLISH}
	 * to a {@link Task} in a {@link Task.Status#SCHEDULED} state
	 *
	 * @param taskMapperContext: A wrapper class containing the
	 *                           {@link WorkflowTask}, {@link WorkflowDef},
	 *                           {@link Workflow} and a string representation of the
	 *                           TaskId
	 * @return a List with just one AMQP task
	 * @throws TerminateWorkflowException In case if the task definition does not
	 *                                    exist
	 */
	@Override
	public List<Task> getMappedTasks(TaskMapperContext taskMapperContext) throws TerminateWorkflowException {

		logger.debug("TaskMapperContext {} in AMQPWaitTaskMapper", taskMapperContext);

		WorkflowTask taskToSchedule = taskMapperContext.getTaskToSchedule();
		Workflow workflowInstance = taskMapperContext.getWorkflowInstance();
		String taskId = taskMapperContext.getTaskId();
		int retryCount = taskMapperContext.getRetryCount();

		TaskDef taskDefinition = Optional.ofNullable(taskMapperContext.getTaskDefinition())
				.orElseGet(() -> Optional.ofNullable(metadataDAO.getTaskDef(taskToSchedule.getName())).orElse(null));

		Map<String, Object> input = parametersUtils.getTaskInputV2(taskToSchedule.getInputParameters(),
				workflowInstance, taskId, taskDefinition);

		Task amqpWaitTask = new Task();
		amqpWaitTask.setTaskType(taskToSchedule.getType());
		amqpWaitTask.setTaskDefName(taskToSchedule.getName());
		amqpWaitTask.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
		amqpWaitTask.setWorkflowInstanceId(workflowInstance.getWorkflowId());
		amqpWaitTask.setWorkflowType(workflowInstance.getWorkflowName());
		amqpWaitTask.setCorrelationId(workflowInstance.getCorrelationId());
		amqpWaitTask.setScheduledTime(System.currentTimeMillis());
		amqpWaitTask.setTaskId(taskId);
		amqpWaitTask.setInputData(input);
		amqpWaitTask.setStatus(Task.Status.SCHEDULED);
		amqpWaitTask.setRetryCount(retryCount);
		amqpWaitTask.setCallbackAfterSeconds(taskToSchedule.getStartDelay());
		amqpWaitTask.setWorkflowTask(taskToSchedule);
		amqpWaitTask.setWorkflowPriority(workflowInstance.getPriority());
		if (Objects.nonNull(taskDefinition)) {
			amqpWaitTask.setRateLimitPerFrequency(taskDefinition.getRateLimitPerFrequency());
			amqpWaitTask.setRateLimitFrequencyInSeconds(taskDefinition.getRateLimitFrequencyInSeconds());
		}
		return Collections.singletonList(amqpWaitTask);
	}
}
