package com.solace.maas.ep.event.management.agent.plugin.terraform.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.maas.ep.event.management.agent.plugin.command.model.Command;
import com.solace.maas.ep.event.management.agent.plugin.command.model.CommandRequest;
import com.solace.maas.ep.event.management.agent.plugin.command.model.CommandResult;
import com.solace.maas.ep.event.management.agent.plugin.command.model.JobStatus;
import com.solace.maas.ep.event.management.agent.plugin.constants.RouteConstants;
import com.solace.maas.ep.event.management.agent.plugin.terraform.client.TerraformClient;
import com.solace.maas.ep.event.management.agent.plugin.terraform.client.TerraformClientFactory;
import com.solace.maas.ep.event.management.agent.plugin.terraform.configuration.TerraformProperties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TerraformManager {
    public static final String LOG_LEVEL_ERROR = "ERROR";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final TerraformLogProcessingService terraformLogProcessingService;
    private final TerraformProperties terraformProperties;
    private final TerraformClientFactory terraformClientFactory;
    private static final String DEFAULT_TF_CONFIG_FILENAME = "config.tf";

    public TerraformManager(TerraformLogProcessingService terraformLogProcessingService,
                            TerraformProperties terraformProperties, TerraformClientFactory terraformClientFactory) {
        this.terraformLogProcessingService = terraformLogProcessingService;
        this.terraformProperties = terraformProperties;
        this.terraformClientFactory = terraformClientFactory;
    }

    public void execute(CommandRequest request, Command command, Map<String, String> envVars) {

        MDC.put(RouteConstants.COMMAND_CORRELATION_ID, request.getCommandCorrelationId());
        MDC.put(RouteConstants.MESSAGING_SERVICE_ID, request.getServiceId());

        log.debug("Executing command {} for serviceId {} correlationId {} context {}", command.getCommand(), request.getServiceId(),
                request.getCommandCorrelationId(), request.getContext());

        try (TerraformClient terraformClient = terraformClientFactory.createClient()) {

            Path configPath = createConfigPath(request);
            List<String> logOutput = setupTerraformClient(terraformClient, configPath);
            String commandVerb = executeTerraformCommand(command, envVars, configPath, terraformClient, logOutput);
            processTerraformResponse(command, commandVerb, logOutput);
        } catch (InterruptedException e) {
            log.error("Received a thread interrupt while executing the terraform command", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("An error was encountered while executing the terraform command", e);
            setCommandError(command, e);
        }
    }

    private static List<String> setupTerraformClient(TerraformClient terraformClient, Path configPath) {
        terraformClient.setWorkingDirectory(configPath.toFile());
        List<String> output = new ArrayList<>();

        // Write each terraform to the output list so that it can be processed later
        // Also write the output to the main log to be streamed back to EP
        terraformClient.setOutputListener(tfLog -> {
            output.add(tfLog);
            logToConsole(tfLog);
        });
        return output;
    }

    @SneakyThrows
    private static void logToConsole(String tfLog) {

        String logMessage = String.format("Terraform output: %s", tfLog);

        Map<String, Object> logMop = objectMapper.readValue(tfLog, Map.class);
        String logLevel = (String) logMop.get("@level");
        switch (logLevel) {
            case "trace" -> log.trace(logMessage);
            case "debug" -> log.debug(logMessage);
            case "info" -> log.info(logMessage);
            case "warn" -> log.warn(logMessage);
            case "error" -> log.error(logMessage);
            default -> log.error("cannot map the logLevel properly for tfLog {}", tfLog);
        }
    }

    private static String executeTerraformCommand(Command command, Map<String, String> envVars, Path configPath,
                                                  TerraformClient terraformClient, List<String> output) throws IOException, InterruptedException, ExecutionException {
        String commandVerb = command.getCommand();
        switch (commandVerb) {
            case "import" -> {
                boolean importPlanSuccessful = terraformClient.plan(envVars).get();

                if (!importPlanSuccessful) {
                    // Re-write the import file to only include the successful imports
                    String successfulImports = output.stream()
                            .map(json -> {
                                try {
                                    return objectMapper.readValue(json, Map.class);
                                } catch (JsonProcessingException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                            .filter(log -> "planned_change".equals(log.get("type")))
                            .map(log -> (Map<String, Object>) log.get("change"))
                            .filter(log -> "import".equals(log.get("action")))
                            .map(log -> new TfImport(
                                    ((Map<String, Object>) log.get("importing")).get("id").toString(),
                                    ((Map<String, Object>) log.get("resource")).get("resource").toString()))
                            .map(TfImport::toString)
                            .collect(Collectors.joining("\n"));

                    Files.writeString(configPath.resolve("import.tf"), successfulImports);
                }
            }

            case "apply" -> {
                writeHclToFile(command, configPath);
                terraformClient.apply(envVars).get();
            }
            case "write_HCL" -> writeHclToFile(command, configPath);
            default -> throw new IllegalArgumentException("Unsupported command " + commandVerb);
        }
        return commandVerb;
    }

    private void processTerraformResponse(Command command, String commandVerb, List<String> output) {
        // Process logs and create the result
        if (Boolean.TRUE.equals(command.getIgnoreResult())) {
            command.setResult(CommandResult.builder()
                    .status(JobStatus.success)
                    .logs(List.of())
                    .build());
        } else {
            if (!"write_HCL".equals(commandVerb)) {
                command.setResult(terraformLogProcessingService.buildTfCommandResult(output));
            } else {
                command.setResult(CommandResult.builder()
                        .status(JobStatus.success)
                        .logs(List.of())
                        .build());
            }
        }
    }

    public static void setCommandError(Command command, Exception e) {
        command.setResult(CommandResult.builder()
                .status(JobStatus.error)
                .logs(List.of(
                        Map.of("message", e.getMessage(),
                                "errorType", e.getClass().getName(),
                                "level", LOG_LEVEL_ERROR,
                                "timestamp", OffsetDateTime.now())))
                .build());
    }

    private Path createConfigPath(CommandRequest request) {
        Path configPath = Paths.get(terraformProperties.getWorkingDirectoryRoot() + File.separator
                + request.getContext()
                + "-"
                + request.getServiceId()
                + File.separator
        );

        if (Files.notExists(configPath)) {
            try {
                Files.createDirectories(configPath);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return configPath;
    }

    private static void writeHclToFile(Command command, Path configPath) throws IOException {
        if (StringUtils.isNotEmpty(command.getBody())) {
            // At the moment, we only support base64 decoding
            Map<String, String> parameters = command.getParameters();
            if (parameters != null && parameters.containsKey("Content-Encoding") && "base64".equals(parameters.get("Content-Encoding"))) {
                byte[] decodedBytes;
                try {
                    decodedBytes = Base64.getDecoder().decode(command.getBody());
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Error decoding base64 content", e);
                }
                String filename = DEFAULT_TF_CONFIG_FILENAME;
                if (parameters.containsKey("Output-File-Name")) {
                    filename = parameters.get("Output-File-Name");
                }
                Files.write(configPath.resolve(filename), decodedBytes);
            } else {
                if (parameters == null || !parameters.containsKey("Content-Encoding")) {
                    throw new IllegalArgumentException("Missing Content-Encoding property in command parameters.");
                }

                throw new IllegalArgumentException("Unsupported encoding type " + parameters.get("Content-Encoding"));
            }
        }
    }

    private static class TfImport {
        public TfImport(String id, String to) {
            this.id = id;
            this.to = to;
        }

        public String id;
        public String to;

        @Override
        public String toString() {
            return "import {\n" +
                    "\tto = " + to + '\n' +
                    "\tid = \"" + id + "\"\n" +
                    '}';
        }
    }

}
