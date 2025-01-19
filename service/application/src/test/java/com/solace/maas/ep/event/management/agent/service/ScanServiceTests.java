package com.solace.maas.ep.event.management.agent.service;

import com.solace.maas.ep.event.management.agent.TestConfig;
import com.solace.maas.ep.event.management.agent.config.plugin.enumeration.MessagingServiceType;
import com.solace.maas.ep.event.management.agent.logging.StreamingAppender;
import com.solace.maas.ep.event.management.agent.plugin.constants.ScanStatus;
import com.solace.maas.ep.event.management.agent.plugin.route.RouteBundle;
import com.solace.maas.ep.event.management.agent.plugin.route.RouteBundleHierarchyStore;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.MessagingServiceEntity;
import com.solace.maas.ep.event.management.agent.repository.model.route.RouteEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanRecipientHierarchyEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanStatusEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanTypeEntity;
import com.solace.maas.ep.event.management.agent.repository.scan.ScanRecipientHierarchyRepository;
import com.solace.maas.ep.event.management.agent.repository.scan.ScanRepository;
import com.solace.maas.ep.event.management.agent.repository.scan.ScanStatusRepository;
import com.solace.maas.ep.event.management.agent.repository.scan.ScanTypeRepository;
import com.solace.maas.ep.event.management.agent.service.logging.LoggingService;
import com.solace.maas.ep.event.management.agent.util.IDGenerator;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.noop.NoopCounter;
import lombok.SneakyThrows;
import org.apache.camel.Processor;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ActiveProfiles("TEST")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = TestConfig.class)
@SuppressWarnings("CPD-START")
public class ScanServiceTests {
    private final String routeId = UUID.randomUUID().toString();

    @Mock
    LoggingService loggingService;

    @Mock
    StreamingAppender streamingAppender;

    @Mock
    private ScanRepository scanRepository;

    @Mock
    private ScanTypeRepository scanTypeRepository;

    @Mock
    private ScanStatusRepository scanStatusRepository;

    @Mock
    private ScanRecipientHierarchyRepository scanRecipientHierarchyRepository;

    @Mock
    private RouteService routeService;

    @Mock
    private ProducerTemplate producerTemplate;

    @Mock
    private ScanRouteService scanRouteService;

    @Mock
    private IDGenerator idGenerator;

    @Produce
    private ProducerTemplate template;

    @InjectMocks
    private ScanService scanService;

    @Autowired
    private ScanServiceHelper scanServiceHelper;

    @Mock
    private MeterRegistry meterRegistry;

    @Test
    @SneakyThrows
    public void testSingleScanWithRouteBundle() {
        RouteBundle consumerGroupsConfiguration = scanServiceHelper.buildRouteBundle("consumerGroupsConfiguration", List.of(), List.of());
        RouteBundle consumerGroups = scanServiceHelper.buildRouteBundle("consumerGroups", List.of(), List.of(consumerGroupsConfiguration));
        RouteBundle topicConfiguration = scanServiceHelper.buildRouteBundle("topicConfiguration", List.of(), List.of());
        RouteBundle overrideTopicConfiguration = scanServiceHelper.buildRouteBundle("overrideTopicConfiguration", List.of(), List.of());

        RouteBundle routeBundleDestination = RouteBundle.builder()
                .messagingServiceId("service1")
                .routeId("log:deadend")
                .scanType("none")
                .destinations(List.of())
                .recipients(List.of())
                .build();

        RouteBundle topicListing = scanServiceHelper.buildRouteBundle("topicListing", List.of(routeBundleDestination),
                List.of(topicConfiguration, overrideTopicConfiguration));
        RouteBundle additionalConsumerGroupConfigPart1 = scanServiceHelper.buildRouteBundle("additionalConsumerGroupConfigPart1",
                List.of(routeBundleDestination), List.of());
        RouteBundle additionalConsumerGroupConfigPart2 = scanServiceHelper.buildRouteBundle("additionalConsumerGroupConfigPart2",
                List.of(routeBundleDestination), List.of());
        RouteBundle additionalConsumerGroupConfigBundle = scanServiceHelper.buildRouteBundle("consumerGroupsConfiguration",
                List.of(routeBundleDestination), List.of(additionalConsumerGroupConfigPart1, additionalConsumerGroupConfigPart2));

        RouteEntity returnedEntity = scanServiceHelper.buildRouteEntity(routeId, "", true);
        ScanEntity scanEntity = scanServiceHelper.buildScanEntity("123", "emaId1", List.of(returnedEntity), null);
        ScanTypeEntity scanType = scanServiceHelper.buildScanTypeEntity("scan1", "scanType", scanEntity, null);
        ScanStatusEntity scanStatus = scanServiceHelper.buildScanStatusEntity("scanstatus1", "dummyStatus");

        when(idGenerator.generateRandomUniqueId())
                .thenReturn("abc123");
        when(routeService.findById(any(String.class)))
                .thenReturn(Optional.of(returnedEntity));
        when(scanRouteService.saveDestinations(any(List.class)))
                .thenReturn(List.of("log:deadend", List.of()));
        when(scanRepository.save(any(ScanEntity.class)))
                .thenReturn(scanEntity);
        when(scanRepository.findById(any(String.class)))
                .thenReturn(Optional.of(scanEntity));
        when(producerTemplate.asyncSend(any(String.class), any(Processor.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(scanRepository.save(scanEntity))
                .thenReturn(scanEntity);
        when(scanTypeRepository.save(scanType))
                .thenReturn(scanType);
        when(scanStatusRepository.save(scanStatus))
                .thenReturn(scanStatus);
        when(scanRecipientHierarchyRepository.save(any(ScanRecipientHierarchyEntity.class)))
                .thenReturn(mock(ScanRecipientHierarchyEntity.class));
        when(meterRegistry.counter(any(), any(String[].class)))
                .thenReturn(new NoopCounter(new Meter.Id("noop", null, null, null, null)));

        scanService.singleScan("orgId",
                List.of(topicListing, consumerGroups, additionalConsumerGroupConfigBundle),
                "groupId",
                "scanId",
                "traceId",
                "actorId",
                mock(MessagingServiceEntity.class),
                "runtimeAgent1");

        ArgumentCaptor<ScanStatusEntity> scanCaptor = ArgumentCaptor.forClass(ScanStatusEntity.class);
        verify(scanStatusRepository, times(8)).save(scanCaptor.capture());

        // Assert that the scanStatus is initialized to "INITIATED" for all scan types
        scanCaptor.getAllValues().stream().forEach(entity -> assertThat(entity.getStatus().equals(ScanStatus.INITIATED.name())));

        assertThatNoException();
    }

    @Test
    @SneakyThrows
    public void testListScans() {
        ScanEntity result = ScanEntity.builder()
                .id("id1")
                .createdAt(Instant.now())
                .messagingService(MessagingServiceEntity.builder()
                        .id("service1")
                        .name("service1")
                        .type("SOLACE")
                        .build())
                .build();

        when(scanRepository.findAll())
                .thenReturn(List.of(result));

        scanService.listScans();

        assertThatNoException();
    }

    @Test
    @SneakyThrows
    public void testFindAll() {
        MessagingServiceEntity messagingService = scanServiceHelper.buildMessagingServiceEntity(
                "service1", "service1", MessagingServiceType.SOLACE.name());
        ScanStatusEntity scanStatus = scanServiceHelper.buildScanStatusEntity("status1", "COMPLETE");
        ScanTypeEntity scanType = scanServiceHelper.buildScanTypeEntity("123", "SOLACE_ALL", null, scanStatus);

        ScanEntity result = ScanEntity.builder()
                .id("id1")
                .createdAt(Instant.now())
                .messagingService(messagingService)
                .scanTypes(List.of(scanType))
                .build();

        Pageable pageable = PageRequest.of(0, 1);

        Page<ScanEntity> results = new PageImpl<>(List.of(result), pageable, 1);

        when(scanRepository.findAll(pageable))
                .thenReturn(results);

        assertThatNoException();
    }

    @Test
    @SneakyThrows
    public void testFindByMessagingServiceId() {
        MessagingServiceEntity messagingService = scanServiceHelper.buildMessagingServiceEntity(
                "service1", "service1", MessagingServiceType.SOLACE.name());
        ScanStatusEntity scanStatus = scanServiceHelper.buildScanStatusEntity("status1", "COMPLETE");
        ScanTypeEntity scanType = scanServiceHelper.buildScanTypeEntity("123", "SOLACE_ALL", null, scanStatus);

        ScanEntity result = ScanEntity.builder()
                .id("id1")
                .createdAt(Instant.now())
                .messagingService(messagingService)
                .scanTypes(List.of(scanType))
                .build();

        Pageable pageable = PageRequest.of(0, 1);

        Page<ScanEntity> results = new PageImpl<>(List.of(result), pageable, 1);

        when(scanRepository.findAllByMessagingServiceId("service1", pageable))
                .thenReturn(results);

        assertThatNoException();
    }

    @Test
    @SneakyThrows
    public void testParseRouteRecipients() {
        RouteBundle brokerConfiguration = RouteBundle.builder()
                .messagingServiceId("service1")
                .routeId("kafkaBrokerConfiguration")
                .scanType("brokerConfiguration")
                .destinations(List.of())
                .recipients(List.of())
                .build();

        RouteBundle clusterConfiguration = RouteBundle.builder()
                .messagingServiceId("service1")
                .routeId("kafkaClusterConfiguration")
                .scanType("clusterConfiguration")
                .destinations(List.of())
                .recipients(List.of(brokerConfiguration))
                .build();

        RouteBundle topicConfiguration = RouteBundle.builder()
                .messagingServiceId("service1")
                .routeId("kafkaTopicConfiguration")
                .scanType("topicConfiguration")
                .destinations(List.of())
                .recipients(List.of())
                .build();

        RouteBundle overrideTopicConfiguration = RouteBundle.builder()
                .messagingServiceId("service1")
                .routeId("kafkaOverrideTopicConfiguration")
                .scanType("overrideTopicConfiguration")
                .destinations(List.of())
                .recipients(List.of())
                .build();

        RouteBundle topicListing = RouteBundle.builder()
                .messagingServiceId("service1")
                .routeId("kafkaDataPublisher")
                .scanType("topicListing")
                .destinations(List.of())
                .recipients(List.of(topicConfiguration, overrideTopicConfiguration))
                .build();

        RouteBundle consumerGroupConfiguration = RouteBundle.builder()
                .messagingServiceId("service1")
                .routeId("kafkaConsumerGroupConfiguration")
                .scanType("consumerGroupConfiguration")
                .destinations(List.of())
                .recipients(List.of())
                .build();

        RouteBundle consumerGroups = RouteBundle.builder()
                .messagingServiceId("service1")
                .routeId("kafkaConsumerGroupDataPublisher")
                .scanType("consumerGroups")
                .destinations(List.of())
                .recipients(List.of(consumerGroupConfiguration))
                .build();

        RouteBundleHierarchyStore routeBundleHierarchyStore =
                scanService.parseRouteRecipients(List.of(clusterConfiguration, topicListing, consumerGroups),
                        new RouteBundleHierarchyStore());

        List<String> routeBundlerHierarchySore = new ArrayList<>(routeBundleHierarchyStore.getStore().values());

        Assertions.assertEquals(4, routeBundlerHierarchySore.size());
        Assertions.assertTrue(routeBundlerHierarchySore.contains("clusterConfiguration,brokerConfiguration"));
        Assertions.assertTrue(routeBundlerHierarchySore.contains("topicListing,topicConfiguration"));
        Assertions.assertTrue(routeBundlerHierarchySore.contains("topicListing,overrideTopicConfiguration"));
        Assertions.assertTrue(routeBundlerHierarchySore.contains("consumerGroups,consumerGroupConfiguration"));
    }

    @Test
    @SneakyThrows
    public void testSendScanStatus() {
        when(meterRegistry.counter(any(), any(String[].class)))
                .thenReturn(new NoopCounter(new Meter.Id("noop", null, null, null, null)));
        ScanService service = new ScanService(mock(ScanRepository.class), mock(ScanRecipientHierarchyRepository.class),
                mock(ScanTypeRepository.class),
                mock(ScanStatusRepository.class), mock(ScanRouteService.class), mock(RouteService.class),
                template, idGenerator, meterRegistry);
        service.sendScanStatus("orgId", "scanId", "groupId", "messagingServiceId", "traceId", "actorId",
                "queueListing", ScanStatus.IN_PROGRESS);

        assertThatNoException();
    }

    @Test
    @SneakyThrows
    public void testFindById() {
        when(scanRepository.findById(any(String.class)))
                .thenReturn(Optional.of(ScanEntity.builder().build()));

        scanService.findById("scan1");

        assertThatNoException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"COMPLETE", "FAILED", "TIMED_OUT"})
    @SneakyThrows
    public void testScanStatusCompletionWithSingleScan(String scanStatus) {
        ScanStatusEntity scanStatusA = scanServiceHelper.buildScanStatusEntity("status1", scanStatus);
        ScanTypeEntity scanTypeA = scanServiceHelper.buildScanTypeEntity("123", "queueListing", null, scanStatusA);

        when(scanTypeRepository.findAllByScanId(any(String.class)))
                .thenReturn(List.of(scanTypeA));
        when(scanStatusRepository.findByScanType(any(ScanTypeEntity.class)))
                .thenReturn(scanStatusA);

        Assertions.assertTrue(scanService.isScanComplete("scan1"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"IN_PROGRESS", "INITIATED"})
    @SneakyThrows
    public void testScanStatusIncompleteWithSingleScan(String scanStatus) {
        ScanStatusEntity scanStatusA = scanServiceHelper.buildScanStatusEntity("status1", scanStatus);
        ScanTypeEntity scanTypeA = scanServiceHelper.buildScanTypeEntity("123", "queueListing", null, scanStatusA);

        when(scanTypeRepository.findAllByScanId(any(String.class)))
                .thenReturn(List.of(scanTypeA));
        when(scanStatusRepository.findByScanType(any(ScanTypeEntity.class)))
                .thenReturn(scanStatusA);

        Assertions.assertFalse(scanService.isScanComplete("scan1"));
    }


    @Test
    @SneakyThrows
    public void testScanStatusCompletedWithMultipleScans() {
        ScanStatusEntity scanStatusA = scanServiceHelper.buildScanStatusEntity("status1", "COMPLETE");
        ScanTypeEntity scanTypeA = scanServiceHelper.buildScanTypeEntity("123", "queueListing", null, scanStatusA);

        ScanStatusEntity scanStatusB = scanServiceHelper.buildScanStatusEntity("status2", "COMPLETE");
        ScanTypeEntity scanTypeB = scanServiceHelper.buildScanTypeEntity("124", "queueListing", null, scanStatusB);

        when(scanTypeRepository.findAllByScanId(any(String.class)))
                .thenReturn(List.of(scanTypeA, scanTypeB));
        when(scanStatusRepository.findByScanType(scanTypeA))
                .thenReturn(scanStatusA);
        when(scanStatusRepository.findByScanType(scanTypeB))
                .thenReturn(scanStatusB);

        Assertions.assertTrue(scanService.isScanComplete("scan1"));
    }

    @Test
    @SneakyThrows
    public void testScanStatusNotCompleteWithMultipleScans() {
        ScanStatusEntity scanStatusA = scanServiceHelper.buildScanStatusEntity("status1", "COMPLETE");
        ScanTypeEntity scanTypeA = scanServiceHelper.buildScanTypeEntity("123", "queueListing", null, scanStatusA);

        ScanStatusEntity scanStatusB = scanServiceHelper.buildScanStatusEntity("status2", "IN_PROGRESS");
        ScanTypeEntity scanTypeB = scanServiceHelper.buildScanTypeEntity("124", "queueConfiguration", null, scanStatusB);

        when(scanTypeRepository.findAllByScanId(any(String.class)))
                .thenReturn(List.of(scanTypeA, scanTypeB));
        when(scanStatusRepository.findByScanType(scanTypeA))
                .thenReturn(scanStatusA);
        when(scanStatusRepository.findByScanType(scanTypeB))
                .thenReturn(scanStatusB);
        Assertions.assertFalse(scanService.isScanComplete("scan1"));
    }

    @Test
    void testScanServiceIsScanCompleteInvalidArgument() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> scanService.isScanComplete(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> scanService.isScanComplete(""));
    }
}
