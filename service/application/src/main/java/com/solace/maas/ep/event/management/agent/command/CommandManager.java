package com.solace.maas.ep.event.management.agent.command;

import com.solace.maas.ep.common.messages.CommandMessage;
import com.solace.maas.ep.event.management.agent.command.mapper.CommandMapper;
import com.solace.maas.ep.event.management.agent.command.semp.SempApiProviderImpl;
import com.solace.maas.ep.event.management.agent.config.eventPortal.EventPortalProperties;
import com.solace.maas.ep.event.management.agent.plugin.command.model.Command;
import com.solace.maas.ep.event.management.agent.plugin.command.model.CommandBundle;
import com.solace.maas.ep.event.management.agent.plugin.command.model.CommandRequest;
import com.solace.maas.ep.event.management.agent.plugin.command.model.CommandResult;
import com.solace.maas.ep.event.management.agent.plugin.command.model.JobStatus;
import com.solace.maas.ep.event.management.agent.plugin.service.MessagingServiceDelegateService;
import com.solace.maas.ep.event.management.agent.plugin.solace.processor.semp.SempClient;
import com.solace.maas.ep.event.management.agent.plugin.solace.processor.semp.SolaceHttpSemp;
import com.solace.maas.ep.event.management.agent.plugin.terraform.manager.TerraformLogProcessingService;
import com.solace.maas.ep.event.management.agent.plugin.terraform.manager.TerraformManager;
import com.solace.maas.ep.event.management.agent.processor.CommandLogStreamingProcessor;
import com.solace.maas.ep.event.management.agent.publisher.CommandPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.Validate;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.solace.maas.ep.common.metrics.ObservabilityConstants.MAAS_EMA_CONFIG_PUSH_EVENT_CYCLE_TIME;
import static com.solace.maas.ep.common.metrics.ObservabilityConstants.MAAS_EMA_CONFIG_PUSH_EVENT_SENT;
import static com.solace.maas.ep.common.metrics.ObservabilityConstants.ORG_ID_TAG;
import static com.solace.maas.ep.common.metrics.ObservabilityConstants.STATUS_TAG;
import static com.solace.maas.ep.event.management.agent.constants.Command.COMMAND_CORRELATION_ID;
import static com.solace.maas.ep.event.management.agent.plugin.command.model.CommandType.semp;
import static com.solace.maas.ep.event.management.agent.plugin.command.model.CommandType.terraform;
import static com.solace.maas.ep.event.management.agent.plugin.command.model.SempDeleteCommandConstants.SEMP_DELETE_OPERATION;
import static com.solace.maas.ep.event.management.agent.plugin.constants.RouteConstants.ACTOR_ID;
import static com.solace.maas.ep.event.management.agent.plugin.constants.RouteConstants.TRACE_ID;
import static com.solace.maas.ep.event.management.agent.plugin.terraform.manager.TerraformUtils.LOG_LEVEL_ERROR;
import static com.solace.maas.ep.event.management.agent.plugin.terraform.manager.TerraformUtils.setCommandError;

@Slf4j
@Service
@ConditionalOnProperty(name = "event-portal.gateway.messaging.standalone", havingValue = "false")
public class CommandManager {
    public static final String ERROR_EXECUTING_COMMAND = "Error executing command";
    private final TerraformManager terraformManager;
    private final CommandMapper commandMapper;
    private final CommandPublisher commandPublisher;
    private final MessagingServiceDelegateService messagingServiceDelegateService;
    private final EventPortalProperties eventPortalProperties;
    private final Optional<CommandLogStreamingProcessor> commandLogStreamingProcessorOpt;
    private final MeterRegistry meterRegistry;
    private final SempDeleteCommandManager sempDeleteCommandManager;
    private final TerraformLogProcessingService terraformLoggingService;

    public CommandManager(TerraformManager terraformManager,
                          CommandMapper commandMapper,
                          CommandPublisher commandPublisher,
                          MessagingServiceDelegateService messagingServiceDelegateService,
                          EventPortalProperties eventPortalProperties,
                          Optional<CommandLogStreamingProcessor> commandLogStreamingProcessorOpt,
                          MeterRegistry meterRegistry,
                          final SempDeleteCommandManager sempDeleteCommandManager,
                          TerraformLogProcessingService terraformLoggingService) {
        this.terraformManager = terraformManager;
        this.commandMapper = commandMapper;
        this.commandPublisher = commandPublisher;
        this.messagingServiceDelegateService = messagingServiceDelegateService;
        this.eventPortalProperties = eventPortalProperties;
        this.meterRegistry = meterRegistry;

        this.commandLogStreamingProcessorOpt = commandLogStreamingProcessorOpt;
        this.sempDeleteCommandManager = sempDeleteCommandManager;
        this.terraformLoggingService = terraformLoggingService;
    }

    public void execute(CommandMessage request) {
        Validate.notNull(request, "CommandRequest must not be null");
        Validate.notEmpty(request.getServiceId(), "ServiceId must not be empty");
        Validate.notEmpty(request.getCommandBundles(), "CommandBundles must not be empty");
        Validate.notEmpty(request.getCommandBundles().get(0).getCommands(), "At least one command must be present");
        CommandRequest requestBO = commandMapper.map(request);
        configPush(request.getOrgId(), requestBO);
    }

    /**
     * Handles the error by attaching the error to the first command in the request and sends the response to EP.
     * Convenience method to be used by the onFailure method of the MessageProcessor to handle the error and send the response to EP.
     *
     * @param e
     * @param message
     */
    public void handleError(Exception e, CommandMessage message) {
        CommandRequest requestBO = commandMapper.map(message);
        attachErrorLogToCommand(false, e, requestBO);
        finalizeAndSendResponse(message.getOrgId(), requestBO);
    }

    @SuppressWarnings("PMD")
    private void configPush(String orgId, CommandRequest request) {
        List<Path> executionLogFilesToClean = new ArrayList<>();
        boolean attacheErrorToTerraformCommand = false;
        try {
            // if the serviceId is not found, messagingServiceDelegateService will most likely throw an exception (which is not guaranteed
            // based on the interface definition) and we need to catch it here.
            // We also need to check if the client is null if there is no exception thrown
            SolaceHttpSemp solaceClient = messagingServiceDelegateService.getMessagingServiceClient(request.getServiceId());
            if (solaceClient == null) {
                throw new IllegalStateException("Messaging service client not found for serviceId " + request.getServiceId());
            }
            Map<String, String> envVars = setBrokerSpecificEnvVars(solaceClient);

            // Delete the terraform state file before running the terraform commands
            // It will delete all files in the directory of this context
            terraformManager.deleteTerraformState(request);

            for (CommandBundle bundle : request.getCommandBundles()) {
                boolean exitEarlyOnFailedCommand = bundle.getExitOnFailure();
                // For now everything is run serially
                for (Command command : bundle.getCommands()) {
                    attacheErrorToTerraformCommand = false;
                    if (command.getCommandType() == semp) {
                        executeSempCommand(command, solaceClient);
                    } else if (command.getCommandType() == terraform) {
                        attacheErrorToTerraformCommand = true;
                        Path executionLog = executeTerraformCommand(request, command, envVars);
                        if (executionLog != null) {
                            if (commandLogStreamingProcessorOpt.isPresent()) {
                                streamCommandExecutionLogToEpCore(request, command, executionLog);
                            }
                            executionLogFilesToClean.add(executionLog);
                        }
                    } else {
                        // attaches an error entry for the command as CommandResult
                        handleUnknownCommandType(command);
                    }
                    if (exitEarlyOnFailedCommand(exitEarlyOnFailedCommand, command)) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("ConfigPush command not executed successfully", e);
            attachErrorLogToCommand(attacheErrorToTerraformCommand, e, request);
        } finally {
            try {
                finalizeAndSendResponse(orgId, request);
                // the response could also be an error log entry
                // if we fail to send the response to EP, we can not do anything about it
                // besides logging it
            } catch (Exception e) {
                log.error("Error while sending response to Event Portal. ", e);
                // at least we can attach the error to the first command
                // this information might be used by the invoker of this method
                attachErrorLogToCommand(false, e, request);
            }
            // cleanup handles errors by logging and swallowing them
            // no need to catch them here
            cleanup(executionLogFilesToClean);
        }
    }

    // Attaches the error log to the first terraform command in the request to be sent back to EP although the error
    // occurred during the deletion of the terraform state file and not during the terraform command execution
    @SuppressWarnings("PMD")
    private void attachErrorLogToCommand(boolean isTerraformRelatedError, Exception ex, CommandRequest request) {
        for (CommandBundle bundle : request.getCommandBundles()) {
            for (Command command : bundle.getCommands()) {
                if (!isTerraformRelatedError) {
                    // attaches the error to the command as CommandResult
                    setCommandError(command, ex);
                    return;
                }
                if (command.getCommandType() == terraform) {
                    // casting or wrapping the exception to RuntimeException
                    // due to backwards compatibility of terraformLoggingService.buildTfStateFileDeletionFailureResult
                    RuntimeException runtimeEx;
                    if (ex instanceof RuntimeException) {
                        runtimeEx = (RuntimeException) ex;
                    } else {
                        runtimeEx = new RuntimeException(ex);
                    }
                    CommandResult errorResult = terraformLoggingService.buildTfStateFileDeletionFailureResult(runtimeEx);
                    command.setResult(errorResult);
                    return;
                }
            }
        }
        if (isTerraformRelatedError) {
            log.error("No terraform command found to attach error log to");
        } else {
            log.error("No command found to attach error log to");
        }
    }

    private void finalizeAndSendResponse(String orgId, CommandRequest request) {
        request.determineStatus();
        Map<String, String> topicVars = Map.of(
                "orgId", orgId,
                "runtimeAgentId", eventPortalProperties.getRuntimeAgentId(),
                COMMAND_CORRELATION_ID, request.getCommandCorrelationId()
        );
        CommandMessage response = new CommandMessage(request.getServiceId(),
                request.getCommandCorrelationId(),
                request.getContext(),
                request.getStatus(),
                request.getCommandBundles());
        response.setOrgId(orgId);
        response.setTraceId(MDC.get(TRACE_ID));
        response.setActorId(MDC.get(ACTOR_ID));
        commandPublisher.sendCommandResponse(response, topicVars);
        meterRegistry.counter(MAAS_EMA_CONFIG_PUSH_EVENT_SENT, ORG_ID_TAG, response.getOrgId(),
                STATUS_TAG, response.getStatus().name()).increment();
        Timer jobCycleTime = Timer
                .builder(MAAS_EMA_CONFIG_PUSH_EVENT_CYCLE_TIME)
                .tag(ORG_ID_TAG, response.getOrgId())
                .tag(STATUS_TAG, request.getStatus().name())
                .register(meterRegistry);
        jobCycleTime.record(request.getLifetime(ChronoUnit.MILLIS), TimeUnit.MILLISECONDS);
    }

    private Path executeTerraformCommand(CommandRequest request, Command command, Map<String, String> envVars) {
        Path executionLog = null;
        try {
            Validate.isTrue(command.getCommandType().equals(terraform), "Command type must be terraform");
            return terraformManager.execute(request, command, envVars);
        } catch (Exception e) {
            log.error(ERROR_EXECUTING_COMMAND, e);
            setCommandError(command, e);

        }
        return executionLog;
    }

    private void executeSempCommand(Command command, SolaceHttpSemp solaceClient) {
        try {
            Validate.isTrue(command.getCommandType().equals(semp), "Command type must be semp");
            // only delete operation is supported for now and only delete operations are sent from EP
            Validate.isTrue(command.getCommand().equals(SEMP_DELETE_OPERATION), "Command operation must be delete");
            // creating a new SempApiProviderImpl instance for each command execution
            // if this becomes a performance issue, we can consider caching the SempApiProviderImpl instance for each serviceId
            sempDeleteCommandManager.execute(command, new SempApiProviderImpl(solaceClient));
        } catch (Exception e) {
            log.error(ERROR_EXECUTING_COMMAND, e);
            setCommandError(command, e);
        }
    }

    private void handleUnknownCommandType(Command command) {
        command.setResult(CommandResult.builder()
                .status(JobStatus.error)
                .logs(List.of(
                        Map.of("message", "unknown command type " + command.getCommandType(),
                                "errorType", "UnknownCommandType",
                                "level", LOG_LEVEL_ERROR,
                                "timestamp", OffsetDateTime.now())))
                .build());
    }

    private void cleanup(List<Path> listOfExecutionLogFiles) {
        try {
            deleteExecutionLogFiles(listOfExecutionLogFiles);
        } catch (Exception e) {
            log.error("Error while deleting execution log.", e);
        }
    }

    public void deleteExecutionLogFiles(List<Path> listOfExecutionLogFiles) {
        boolean allFilesDeleted = listOfExecutionLogFiles
                .stream()
                .allMatch(this::deleteExecutionLogFile);
        if (!allFilesDeleted) {
            throw new IllegalArgumentException("Some of the execution log files were not deleted. Please check the logs");
        }
    }

    private boolean deleteExecutionLogFile(Path path) {
        try {
            if (Files.exists(path)) {
                Files.delete(path);
            }
        } catch (IOException e) {
            log.warn("Error while deleting execution log at {}", path, e);
            return false;
        }
        return true;
    }

    public void streamCommandExecutionLogToEpCore(CommandRequest request, Command command, Path executionLog) {
        if (commandLogStreamingProcessorOpt.isEmpty()) {
            throw new UnsupportedOperationException("Streaming logs to ep is not supported for this event management agent type");
        }
        try {
            commandLogStreamingProcessorOpt.get().streamLogsToEP(request, command, executionLog);
        } catch (Exception e) {
            log.error("Error sending logs to ep-core for command with commandCorrelationId {}",
                    request.getCommandCorrelationId(), e);
        }
    }


    private static boolean exitEarlyOnFailedCommand(boolean existEarlyOnFailedCommand, Command command) {
        return Boolean.TRUE.equals(existEarlyOnFailedCommand)
                && Boolean.FALSE.equals(command.getIgnoreResult())
                && (command.getResult() == null || JobStatus.error.equals(command.getResult().getStatus()));
    }


    private Map<String, String> setBrokerSpecificEnvVars(SolaceHttpSemp solaceClient) {
        Map<String, String> envVars = new HashMap<>();
        SempClient sempClient = solaceClient.getSempClient();
        envVars.put("TF_VAR_username", sempClient.getUsername());
        envVars.put("TF_VAR_password", sempClient.getPassword());
        envVars.put("TF_VAR_url", sempClient.getConnectionUrl());
        return envVars;
    }


}
