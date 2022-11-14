package com.solace.maas.ep.event.management.agent.route.manualImport;

import com.solace.maas.ep.event.management.agent.processor.ScanDataPublishImportScanEventProcessor;
import com.solace.maas.ep.event.management.agent.route.ep.exceptionHandlers.ScanDataImportExceptionHandler;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnExpression("${eventPortal.gateway.messaging.standalone} == false")
public class ImportRouteBuilder extends RouteBuilder {
    private final ScanDataPublishImportScanEventProcessor scanDataImportPublishProcessor;

    public ImportRouteBuilder(ScanDataPublishImportScanEventProcessor scanDataImportPublishProcessor) {
        this.scanDataImportPublishProcessor = scanDataImportPublishProcessor;
    }

    @Override
    public void configure() {

        from("seda:importScanData?blockWhenFull=true&size=100")
                .routeId("importScanData")
                .onException(Exception.class)
                .process(new ScanDataImportExceptionHandler())
                .continued(true)
                .end()
                .process(scanDataImportPublishProcessor)
                .setProperty("IMPORT_ID", header("IMPORT_ID"))
                .setHeader(Exchange.FILE_NAME, simple("${header.IMPORT_ID}.zip"))
                .toD("file://data_collection/import/compressed_data_collection")
                .to("direct:checkZipSizeAndUnzipFiles");

        from("direct:continueParsingUnzippedFiles")
                .to("direct:parseMetaInfoAndSendOverAllImportStatus")
                .to("direct:parseAndStreamImportFiles");
    }
}
