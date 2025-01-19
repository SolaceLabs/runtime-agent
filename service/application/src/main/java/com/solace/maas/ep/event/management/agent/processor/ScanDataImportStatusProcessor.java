package com.solace.maas.ep.event.management.agent.processor;

import com.solace.maas.ep.event.management.agent.plugin.constants.RouteConstants;
import com.solace.maas.ep.event.management.agent.plugin.constants.ScanStatus;
import com.solace.maas.ep.event.management.agent.scanManager.model.MetaInfFileDetailsBO;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@ConditionalOnProperty(name = "event-portal.gateway.messaging.standalone", havingValue = "false")
public class  ScanDataImportStatusProcessor implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {
        MetaInfFileDetailsBO fileName = (MetaInfFileDetailsBO) exchange.getIn().getBody();
        String scanType = fileName.getDataEntityType();

        exchange.getIn().setHeader(RouteConstants.SCAN_STATUS, ScanStatus.IN_PROGRESS);
        exchange.getIn().setHeader(RouteConstants.SCAN_TYPE, scanType);
    }
}
