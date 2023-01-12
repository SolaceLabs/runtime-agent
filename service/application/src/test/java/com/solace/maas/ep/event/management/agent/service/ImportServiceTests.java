package com.solace.maas.ep.event.management.agent.service;

import com.solace.maas.ep.event.management.agent.TestConfig;
import com.solace.maas.ep.event.management.agent.config.eventPortal.EventPortalProperties;
import com.solace.maas.ep.event.management.agent.config.eventPortal.GatewayMessagingProperties;
import com.solace.maas.ep.event.management.agent.config.eventPortal.GatewayProperties;
import com.solace.maas.ep.event.management.agent.repository.model.file.DataCollectionFileEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanEntity;
import com.solace.maas.ep.event.management.agent.scanManager.model.ImportRequestBO;
import com.solace.maas.ep.event.management.agent.scanManager.model.ZipRequestBO;
import lombok.SneakyThrows;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.support.DefaultExchange;
import org.apache.tomcat.util.http.fileupload.FileUploadException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ActiveProfiles("TEST")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = TestConfig.class)
@SuppressWarnings("CPD-START")
public class ImportServiceTests {
    @TempDir
    Path tempDir;

    @Mock
    EventPortalProperties eventPortalProperties;

    @Autowired
    private CamelContext camelContext;

    @Mock
    private ProducerTemplate producerTemplate;

    @Mock
    private DataCollectionFileService dataCollectionFileService;

    @InjectMocks
    private ImportService importService;

    @SneakyThrows
    @Test
    public void testImportData() {
        MultipartFile multipartFile = new MockMultipartFile("file.tmp", "test".getBytes());

        ImportRequestBO importRequestBO = ImportRequestBO.builder()
                .dataFile(multipartFile)
                .build();

        when(eventPortalProperties.getGateway())
                .thenReturn(GatewayProperties.builder()
                        .messaging(GatewayMessagingProperties.builder().standalone(false).build())
                        .build());

        importService.importData(importRequestBO);

        assertThatNoException();
    }

    @SneakyThrows
    @Test
    public void testImportDataInStandAloneMode() {
        MultipartFile multipartFile = new MockMultipartFile("file.tmp", "test".getBytes());

        ImportRequestBO importRequestBO = ImportRequestBO.builder()
                .dataFile(multipartFile)
                .build();

        when(eventPortalProperties.getGateway())
                .thenReturn(GatewayProperties.builder()
                        .messaging(GatewayMessagingProperties.builder().standalone(true).build())
                        .build());

        Exception exception = assertThrows(FileUploadException.class, () -> importService.importData(importRequestBO));
        assertTrue(exception.getMessage().contains("Scan data could not be imported in standalone mode."));
    }

    @SneakyThrows
    @Test
    public void testZipData() {
        ZipRequestBO zipRequestBO = ZipRequestBO.builder()
                .messagingServiceId("service1")
                .scanId("scanId")
                .build();

        when(dataCollectionFileService.findAllByScanId("scanId"))
                .thenReturn(List.of(DataCollectionFileEntity.builder()
                        .id(UUID.randomUUID().toString())
                        .path("data_collection/" + UUID.randomUUID() + "/" + UUID.randomUUID() + "/topicListing.json")
                        .scan(ScanEntity.builder()
                                .id(UUID.randomUUID().toString())
                                .active(true)
                                .scanType("KAFKA_ALL")
                                .build())
                        .purged(false)
                        .build()));

        Path file = tempDir.resolve("test.json");
        Files.write(file, Collections.singleton("test data"));

        GenericFile<File> genericFile = new GenericFile<>(false);
        genericFile.setFile(file.toFile());

        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(genericFile);

        when(producerTemplate.send(any(String.class), any(Processor.class)))
                .thenReturn(exchange);

        importService.zip(zipRequestBO);

        assertThatNoException();
    }
}
