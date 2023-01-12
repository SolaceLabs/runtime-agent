package com.solace.maas.ep.event.management.agent.route.ep;

import com.solace.maas.ep.event.management.agent.plugin.constants.RouteConstants;
import com.solace.maas.ep.event.management.agent.processor.ScanStatusOverAllProcessor;
import com.solace.maas.ep.event.management.agent.processor.ScanStatusPerRouteProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnExpression("${eventPortal.gateway.messaging.standalone} == false")
@Profile("!TEST")
public class ScanStatusPublisherRouteBuilder extends AbstractRouteBuilder {

    private final ScanStatusOverAllProcessor scanStatusOverallProcessor;

    private final ScanStatusPerRouteProcessor scanStatusPerRouteProcessor;


    @Autowired
    public ScanStatusPublisherRouteBuilder(ScanStatusOverAllProcessor scanStatusOverallProcessor,
                                           ScanStatusPerRouteProcessor scanStatusPerRouteProcessor) {
        super();
        this.scanStatusOverallProcessor = scanStatusOverallProcessor;
        this.scanStatusPerRouteProcessor = scanStatusPerRouteProcessor;
    }

    @Override
    public void configure() throws Exception {
        super.configure();

        from("direct:perRouteScanStatusPublisher")
                .routeId("perRouteScanStatusPublisher")
                .process(scanStatusPerRouteProcessor)
                .to("bean:scanStatusPublisher?method=sendScanDataStatus(" +
                        "${header." + RouteConstants.SCAN_DATA_STATUS_MESSAGE + "}," +
                        "${header." + RouteConstants.TOPIC_DETAILS + "})");

        from("direct:overallScanStatusPublisher")
                .routeId("overallScanStatusPublisher")
                .process(scanStatusOverallProcessor)
                .to("bean:scanStatusPublisher?method=sendOverallScanStatus(" +
                        "${header." + RouteConstants.GENERAL_STATUS_MESSAGE + "}," +
                        "${header." + RouteConstants.TOPIC_DETAILS + "})");
    }
}
