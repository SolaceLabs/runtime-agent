package com.solace.maas.ep.event.management.agent.route.manualImport;

import com.solace.maas.ep.event.management.agent.processor.ScanDataImportOverAllStatusProcessor;
import com.solace.maas.ep.event.management.agent.processor.ScanDataImportParseMetaInfFileProcessor;
import com.solace.maas.ep.event.management.agent.processor.ScanDataImportPublishImportScanEventProcessor;
import com.solace.maas.ep.event.management.agent.route.ep.exceptionhandlers.ScanDataImportExceptionHandler;
import com.solace.maas.ep.event.management.agent.scanManager.model.MetaInfFileBO;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.processor.aggregate.UseLatestAggregationStrategy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnExpression("${eventPortal.gateway.messaging.standalone} == false")
public class ScanDataImportParseMetaInfFileRouteBuilder extends RouteBuilder {

    private final ScanDataImportParseMetaInfFileProcessor scanDataImportParseMetaInfFileProcessor;
    private final ScanDataImportOverAllStatusProcessor scanDataImportOverAllStatusProcessor;
    private final ScanDataImportPublishImportScanEventProcessor scanDataImportPublishImportScanEventProcessor;

    public ScanDataImportParseMetaInfFileRouteBuilder(ScanDataImportParseMetaInfFileProcessor scanDataImportParseMetaInfFileProcessor,
                                                      ScanDataImportOverAllStatusProcessor scanDataImportOverAllStatusProcessor,
                                                      ScanDataImportPublishImportScanEventProcessor scanDataImportPublishImportScanEventProcessor) {
        this.scanDataImportParseMetaInfFileProcessor = scanDataImportParseMetaInfFileProcessor;
        this.scanDataImportOverAllStatusProcessor = scanDataImportOverAllStatusProcessor;
        this.scanDataImportPublishImportScanEventProcessor = scanDataImportPublishImportScanEventProcessor;
    }

    @Override
    public void configure() {

        from("direct:parseMetaInfoAndSendOverAllImportStatus")
                .routeId("parseMetaInfoAndSendOverAllImportStatus")
                .onException(Exception.class)
                .process(new ScanDataImportExceptionHandler())
                .continued(true)
                .end()
                .pollEnrich()
                .simple("file://data_collection/import/unzipped_data_collection/${header.IMPORT_ID}" +
                        "?fileName=META_INF.json&noop=true&idempotent=false")
                .aggregationStrategy(new UseLatestAggregationStrategy())
                .convertBodyTo(String.class)
                .unmarshal().json(JsonLibrary.Jackson, MetaInfFileBO.class)
                .process(scanDataImportParseMetaInfFileProcessor)
                .process(scanDataImportPublishImportScanEventProcessor)
                .to("direct:sendOverAllInProgressImportStatus");

        from("direct:sendOverAllInProgressImportStatus")
                .routeId("sendOverAllInProgressImportStatus")
                .process(scanDataImportOverAllStatusProcessor)
                .to("direct:overallScanStatusPublisher?block=false&failIfNoConsumers=false");
    }
}
