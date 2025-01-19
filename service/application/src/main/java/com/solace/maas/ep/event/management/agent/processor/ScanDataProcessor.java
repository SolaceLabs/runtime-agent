package com.solace.maas.ep.event.management.agent.processor;

import com.solace.maas.ep.common.messages.ScanDataMessage;
import com.solace.maas.ep.event.management.agent.config.eventPortal.EventPortalProperties;
import com.solace.maas.ep.event.management.agent.plugin.constants.RouteConstants;
import com.solace.maas.ep.event.management.agent.plugin.route.exceptions.ScanDataException;
import com.solace.maas.ep.event.management.agent.publisher.ScanDataPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@ConditionalOnProperty(name = "event-portal.gateway.messaging.standalone", havingValue = "false")
public class ScanDataProcessor implements Processor {

    private final ScanDataPublisher scanDataPublisher;
    private final String runtimeAgentId;

    @Autowired
    public ScanDataProcessor(ScanDataPublisher scanDataPublisher, EventPortalProperties eventPortalProperties) {
        super();

        this.scanDataPublisher = scanDataPublisher;
        runtimeAgentId = eventPortalProperties.getRuntimeAgentId();
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        Map<String, String> topicDetails = new HashMap<>();

        Map<String, Object> properties = exchange.getIn().getHeaders();
        String body = (String) exchange.getIn().getBody();

        String messagingServiceId = (String) properties.get(RouteConstants.MESSAGING_SERVICE_ID);
        String scanId = (String) properties.get(RouteConstants.SCAN_ID);
        String traceId = (String) properties.get(RouteConstants.TRACE_ID);
        String actorId = (String) properties.get(RouteConstants.ACTOR_ID);
        String scanType = (String) properties.get(RouteConstants.SCAN_TYPE);
        Boolean isImportOp = (Boolean) properties.get(RouteConstants.IS_DATA_IMPORT);
        String orgId = (String) properties.get(RouteConstants.ORG_ID);

        ScanDataMessage scanDataMessage = new ScanDataMessage(orgId, scanId, traceId, actorId, scanType, body, Instant.now().toString());

        topicDetails.put("orgId", orgId);
        topicDetails.put("runtimeAgentId", runtimeAgentId);
        topicDetails.put("messagingServiceId", messagingServiceId);
        topicDetails.put("scanId", scanId);
        topicDetails.put("scanType", scanType);
        topicDetails.put("isImportOp", String.valueOf(isImportOp));

        try {
            scanDataPublisher.sendScanData(scanDataMessage, topicDetails);
        } catch (Exception e) {
            throw new ScanDataException("Scan data exception: " + e, Map.of(scanId, List.of(e)), body);
        }
    }
}
