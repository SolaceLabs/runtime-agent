package com.solace.maas.ep.event.management.agent.cli;

import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.MessagingServiceEntity;
import com.solace.maas.ep.event.management.agent.scanManager.ScanManager;
import com.solace.maas.ep.event.management.agent.scanManager.model.ScanRequestBO;
import com.solace.maas.ep.event.management.agent.scanManager.model.ZipRequestBO;
import com.solace.maas.ep.event.management.agent.service.ImportService;
import com.solace.maas.ep.event.management.agent.service.MessagingServiceDelegateServiceImpl;
import com.solace.maas.ep.event.management.agent.util.IDGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

@Slf4j
@Component
public class CommandLineScan {

    private final ScanManager scanManager;
    private final IDGenerator idGenerator;
    private final MessagingServiceDelegateServiceImpl messagingServiceDelegateService;
    private final ImportService importService;
    private final CommandLineCommon commandLineCommon;

    public CommandLineScan(ScanManager scanManager,
                           IDGenerator idGenerator,
                           MessagingServiceDelegateServiceImpl messagingServiceDelegateService,
                           ImportService importService,
                           CommandLineCommon commandLineCommon) {
        this.scanManager = scanManager;
        this.idGenerator = idGenerator;
        this.messagingServiceDelegateService = messagingServiceDelegateService;
        this.importService = importService;
        this.commandLineCommon = commandLineCommon;
    }

    public void runScan(String orgId, String messagingServiceId, String filePathAndName) throws InterruptedException, IOException {
        MessagingServiceEntity messagingServiceEntity = messagingServiceDelegateService.getMessagingServiceById(messagingServiceId);

        ScanRequestBO scanRequestBO = ScanRequestBO.builder()
                .messagingServiceId(messagingServiceId)
                .scanId(idGenerator.generateRandomUniqueId())
                .orgId(orgId)
                .destinations(List.of("FILE_WRITER"))
                .build();
        setScanType(messagingServiceEntity, scanRequestBO, messagingServiceId);

        log.info("Scan request [{}]: Received, request details: {}", scanRequestBO.getScanId(), scanRequestBO);
        String scanId = scanManager.scan(scanRequestBO);

        log.info("Scan request [{}]: Scan started.", scanId);
        commandLineCommon.waitForOperationToComplete(scanId);

        if (commandLineCommon.operationSuccesful(scanId)) {
            log.info("Scan request [{}]: Scan completed successfully.", scanId);
            writeScanToZipFile(filePathAndName, scanId);
        } else {
            log.error("Scan request [{}]: Scan did not complete successfully.", scanId);
        }
    }

    private void setScanType(MessagingServiceEntity messagingServiceEntity, ScanRequestBO scanRequestBO, String messagingServiceId) {
        switch (messagingServiceEntity.getType().toLowerCase()) {
            case "solace":
                scanRequestBO.setScanTypes(List.of("SOLACE_ALL"));
                break;
            case "kafka":
                messagingServiceDelegateService.getMessagingServicesRelations(messagingServiceId).stream()
                        .findFirst()
                        .ifPresentOrElse(messagingServiceEntity1 -> scanRequestBO.setScanTypes(List.of("KAFKA_ALL", "CONFLUENT_SCHEMA_REGISTRY_SCHEMA")),
                                () -> scanRequestBO.setScanTypes(List.of("KAFKA_ALL")));
                break;
            case "confluent_schema_registry":
                scanRequestBO.setScanTypes(List.of("CONFLUENT_SCHEMA_REGISTRY_SCHEMA"));
                break;
            default:
                throw new RuntimeException("Unsupported messaging service type: " + messagingServiceEntity.getType());
        }
    }


    private void writeScanToZipFile(String filePathAndName, String scanId) throws IOException {
        // Use the import service to receive the scan stream and write
        // it to a zip file
        ZipRequestBO zipRequestBO = new ZipRequestBO();
        zipRequestBO.setScanId(scanId);
        log.info("Received zip request for scan id: {}", zipRequestBO.getScanId());
        try (InputStream fileStream = importService.zip(zipRequestBO)) {

            File file = new File(filePathAndName);
            writeInputStreamToFile(fileStream, file);
        }
    }

    private static void writeInputStreamToFile(InputStream inputStream, File file) {
        try {
            Files.copy(inputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
