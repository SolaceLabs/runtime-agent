package com.solace.maas.ep.event.management.agent.subscriber.messageProcessors;

import com.solace.maas.ep.common.messages.ScanCommandMessage;
import com.solace.maas.ep.event.management.agent.config.eventPortal.EventPortalProperties;
import com.solace.maas.ep.event.management.agent.scanManager.ScanManager;
import com.solace.maas.ep.event.management.agent.scanManager.model.ScanRequestBO;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import net.logstash.logback.encoder.org.apache.commons.lang3.StringUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.awaitility.core.ConditionTimeoutException;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.solace.maas.ep.common.metrics.ObservabilityConstants.MAAS_EMA_SCAN_EVENT_RECEIVED;
import static com.solace.maas.ep.common.metrics.ObservabilityConstants.SCAN_ID_TAG;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@Slf4j
@Component
@ConditionalOnProperty(name = "event-portal.gateway.messaging.standalone", havingValue = "false")
public class ScanCommandMessageProcessor implements MessageProcessor<ScanCommandMessage> {

    private static final String DEFAULT_DESTINATION = "FILE_WRITER";
    private final ScanManager scanManager;
    private final DynamicResourceConfigurationHelper dynamicResourceConfigurationHelper;
    private final MeterRegistry meterRegistry;
    private final EventPortalProperties eventPortalProperties;

    public ScanCommandMessageProcessor(ScanManager scanManager,
                                       EventPortalProperties eventPortalProperties,
                                       DynamicResourceConfigurationHelper dynamicResourceConfigurationHelper,
                                       MeterRegistry meterRegistry) {
        this.scanManager = scanManager;
        this.eventPortalProperties = eventPortalProperties;
        this.dynamicResourceConfigurationHelper = dynamicResourceConfigurationHelper;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void processMessage(ScanCommandMessage message) {
        MDC.clear();
        String scanId = StringUtils.isEmpty(message.getScanId()) ? UUID.randomUUID().toString() : message.getScanId();
        meterRegistry.counter(MAAS_EMA_SCAN_EVENT_RECEIVED, SCAN_ID_TAG, scanId).increment();

        List<String> destinations = new ArrayList<>();
        List<String> entityTypes = new ArrayList<>();

        log.debug("Received scan command message: {} for event broker: {}, traceId: {}",
                message, message.getMessagingServiceId(), message.getTraceId());

        if (CollectionUtils.isNotEmpty(message.getResources())) {
            dynamicResourceConfigurationHelper.loadSolaceBrokerResourceConfigurations(message.getResources());
        }

        message.getScanTypes().forEach(scanType -> entityTypes.add(scanType.name()));

        if (message.getDestinations() == null) {
            destinations.add(DEFAULT_DESTINATION);
        } else {
            message.getDestinations().forEach(destination -> destinations.add(destination.name()));
            if (!destinations.contains(DEFAULT_DESTINATION)) {
                destinations.add(DEFAULT_DESTINATION);
            }
        }

        List<String> scanRequestDestinations = destinations.stream()
                .distinct().toList();

        ScanRequestBO scanRequestBO = ScanRequestBO.builder()
                .messagingServiceId(message.getMessagingServiceId())
                .scanId(scanId)
                .traceId(message.getTraceId())
                .orgId(message.getOrgId())
                .actorId(message.getActorId())
                .scanTypes(entityTypes)
                .destinations(scanRequestDestinations)
                .build();

        log.info("Received scan request {}. Request details: {}", scanRequestBO.getScanId(), scanRequestBO);

        scanManager.scan(scanRequestBO);
        //if managed, wait for scan to complete
        if (Boolean.TRUE.equals(eventPortalProperties.getManaged())) {
            log.debug("Waiting for scan to complete for scanId: {}", scanId);
            waitForScanCompletion(scanId);
        }
    }

    public void waitForScanCompletion(String scanId) {
        try {
            await()
                    .atMost(eventPortalProperties.getWaitAckScanCompleteTimeoutSec(), SECONDS)
                    .pollInterval(eventPortalProperties.getWaitAckScanCompletePollIntervalSec(), SECONDS)
                    .pollInSameThread()
                    .until(() -> {
                        try {
                            log.debug("Checking if scan with id {} is completed", scanId);
                            return waitUntilScanIsCompleted(scanId);
                        } catch (Exception e) {
                            log.error("Error while waiting for scan to complete", e);
                            return false;
                        }
                    });
        } catch (ConditionTimeoutException e) {
            // Handle the timeout scenario as needed
            log.error("Scan with id {} did not complete within the expected time", scanId);
        }
    }

    private boolean waitUntilScanIsCompleted(String scanId) {
        return scanManager.isScanComplete(scanId);
    }

    @Override
    public Class supportedClass() {
        return ScanCommandMessage.class;
    }

    @Override
    public ScanCommandMessage castToMessageClass(Object message) {
        return (ScanCommandMessage) message;
    }

    @Override
    public void onFailure(Exception e, ScanCommandMessage message) {
        scanManager.handleError(message.getOrgId(), message);
    }
}
