package com.solace.maas.ep.event.management.agent.service;

import com.solace.maas.ep.event.management.agent.plugin.constants.RouteConstants;
import com.solace.maas.ep.event.management.agent.plugin.constants.ScanStatus;
import com.solace.maas.ep.event.management.agent.plugin.constants.SchedulerConstants;
import com.solace.maas.ep.event.management.agent.plugin.constants.SchedulerType;
import com.solace.maas.ep.event.management.agent.plugin.route.RouteBundle;
import com.solace.maas.ep.event.management.agent.plugin.route.RouteBundleHierarchyStore;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.MessagingServiceEntity;
import com.solace.maas.ep.event.management.agent.repository.model.route.RouteEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanDestinationEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanRecipientEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanRecipientHierarchyEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanStatusEntity;
import com.solace.maas.ep.event.management.agent.repository.model.scan.ScanTypeEntity;
import com.solace.maas.ep.event.management.agent.repository.scan.ScanRecipientHierarchyRepository;
import com.solace.maas.ep.event.management.agent.repository.scan.ScanRepository;
import com.solace.maas.ep.event.management.agent.repository.scan.ScanStatusRepository;
import com.solace.maas.ep.event.management.agent.repository.scan.ScanTypeRepository;
import com.solace.maas.ep.event.management.agent.scanManager.model.ScanItemBO;
import com.solace.maas.ep.event.management.agent.scanManager.model.ScanTypeBO;
import com.solace.maas.ep.event.management.agent.util.IDGenerator;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import net.logstash.logback.encoder.org.apache.commons.lang3.StringUtils;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.MDC;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.solace.maas.ep.common.metrics.ObservabilityConstants.MAAS_EMA_SCAN_EVENT_SENT;
import static com.solace.maas.ep.common.metrics.ObservabilityConstants.SCAN_ID_TAG;
import static com.solace.maas.ep.common.metrics.ObservabilityConstants.STATUS_TAG;

/**
 * Responsible for initiating and managing Messaging Service scans.
 */
@Slf4j
@Service
public class ScanService {
    private final ScanRepository repository;

    private final ScanRecipientHierarchyRepository scanRecipientHierarchyRepository;

    private final ScanTypeRepository scanTypeRepository;

    private final ScanStatusRepository scanStatusRepository;

    private final ScanRouteService scanRouteService;

    private final RouteService routeService;

    private final ProducerTemplate producerTemplate;

    private final IDGenerator idGenerator;

    private final MeterRegistry meterRegistry;

    public ScanService(ScanRepository repository,
                       ScanRecipientHierarchyRepository scanRecipientHierarchyRepository,
                       ScanTypeRepository scanTypeRepository,
                       ScanStatusRepository scanStatusRepository,
                       ScanRouteService scanRouteService,
                       RouteService routeService,
                       ProducerTemplate producerTemplate,
                       IDGenerator idGenerator,
                       MeterRegistry meterRegistry) {
        this.repository = repository;
        this.scanRecipientHierarchyRepository = scanRecipientHierarchyRepository;
        this.scanTypeRepository = scanTypeRepository;
        this.scanStatusRepository = scanStatusRepository;
        this.scanRouteService = scanRouteService;
        this.routeService = routeService;
        this.producerTemplate = producerTemplate;
        this.idGenerator = idGenerator;
        this.meterRegistry = meterRegistry;
    }

    /**
     * The concept of a RouteBundle is introduced to make chaining routes easier
     * <p>
     * Each RouteBundle contains a routeId and scanType for a route to be executed,
     * plus a list of destinations and recipients.
     * <p>
     * A destination is another route that is called after the route described by the routeId
     * and scanId is completed. Destination routes cannot be chained.
     * <p>
     * A recipient is another RouteBundle and is the mechanism by which routes are chained
     * together into a scan.
     * <p>
     * The following code is an example of how 3 routes can be chained together for a single scan.
     * <p>
     * RouteBundle fileWriterDestination = RouteBundle.builder()
     * .destinations(List.of())
     * .scanType("")
     * .routeId("seda:dataCollectionFileWrite")
     * .recipients(List.of())
     * .messagingServiceId(myMessagingService.getId())
     * .firstRouteInChain(false)
     * .build();
     * <p>
     * RouteBundle loggingDestination = RouteBundle.builder()
     * .destinations(List.of())
     * .scanType("")
     * .routeId("log:test")
     * .recipients(List.of())
     * .messagingServiceId(myMessagingService.getId())
     * .firstRouteInChain(false)
     * .build();
     * <p>
     * RouteBundle solaceSubscriptionConfiguration = RouteBundle.builder()
     * .destinations(List.of(fileWriterDestination))
     * .scanType("subscriptionConfiguration")
     * .routeId("solaceSubscriptionConfiguration")
     * .recipients(List.of())
     * .messagingServiceId(myMessagingService.getId())
     * .firstRouteInChain(false)
     * .build();
     * <p>
     * RouteBundle solaceQueueListing = RouteBundle.builder()
     * .destinations(List.of(loggingDestination))
     * .scanType("queueListing")
     * .routeId("solaceDataPublisher")
     * .recipients(List.of(solaceSubscriptionConfiguration))
     * .messagingServiceId(myMessagingService.getId())
     * .firstRouteInChain(true)
     * .build();
     * <p>
     * RouteBundle solaceQueueConfiguration = RouteBundle.builder()
     * .destinations(List.of(fileWriterDestination))
     * .scanType("queueConfiguration")
     * .routeId("solaceQueueConfiguration")
     * .recipients(List.of())
     * .messagingServiceId(myMessagingService.getId())
     * .firstRouteInChain(true)
     * .build();
     *
     * @param routeBundles - see description above
     * @return The id of the scan.
     */
    public String singleScan(String orgId,
                             List<RouteBundle> routeBundles,
                             String groupId,
                             String scanId,
                             String traceId,
                             String actorId,
                             MessagingServiceEntity messagingServiceEntity,
                             String runtimeAgentId) {
        log.info("Scan request [{}], trace ID [{}]: Starting a single scan.", scanId, traceId);

        List<String> scanTypes = parseRouteBundle(routeBundles, new ArrayList<>());

        RouteBundleHierarchyStore routeBundleHierarchy =
                parseRouteRecipients(routeBundles, new RouteBundleHierarchyStore());

        save(routeBundleHierarchy, scanId);

        log.info("Scan request [{}], trace ID [{}]: Total of {} scan types to be retrieved: [{}].",
                scanId, traceId, scanTypes.size(), StringUtils.join(scanTypes, ", "));

        sendScanStatus(orgId, groupId, scanId, traceId, actorId, routeBundles.stream().findFirst().orElseThrow().getMessagingServiceId(),
                StringUtils.join(scanTypes, ","), ScanStatus.IN_PROGRESS);

        log.trace("RouteBundles to be processed: {}", routeBundles);

        String scanEntityId = Objects.requireNonNullElseGet(scanId, () -> UUID.randomUUID().toString());

        ScanEntity returnedScanEntity = setupScan(scanEntityId, traceId, actorId, messagingServiceEntity, runtimeAgentId);

        for (RouteBundle routeBundle : routeBundles) {
            log.trace("Processing RouteBundles: {}", routeBundle);

            RouteEntity route = routeService.findById(routeBundle.getRouteId())
                    .orElseThrow();

            updateScan(route, routeBundle, returnedScanEntity);

            scanAsync(orgId, groupId, scanEntityId, traceId, actorId, route, routeBundle.getMessagingServiceId());
        }

        return scanId;
    }

    @Transactional
    protected ScanEntity setupScan(String scanId, String traceId, String actorId, MessagingServiceEntity messagingServiceEntity, String emaId) {
        return saveScanEntity(scanId, traceId, actorId, messagingServiceEntity, emaId);
    }

    @Transactional
    protected void updateScan(RouteEntity route, RouteBundle routeBundle, ScanEntity scanEntity) {
        List<ScanDestinationEntity> destinationEntities = routeBundle.getDestinations().stream()
                .map(destination -> ScanDestinationEntity.builder()
                        .scan(scanEntity)
                        .route(route)
                        .destination(destination.getRouteId())
                        .build())
                .collect(Collectors.toUnmodifiableList());

        if (!destinationEntities.isEmpty()) {
            scanRouteService.saveDestinations(destinationEntities);
        }

        List<ScanRecipientEntity> recipientEntities = routeBundle.getRecipients().stream()
                .map(recipient -> ScanRecipientEntity.builder()
                        .scan(scanEntity)
                        .route(route)
                        .recipient("seda:" + recipient.getRouteId())
                        .build())
                .collect(Collectors.toUnmodifiableList());

        if (!recipientEntities.isEmpty()) {
            scanRouteService.saveRecipients(recipientEntities);
        }

        setScanType(routeBundle, scanEntity);

        setupRecipientsForScan(scanEntity, routeBundle);
    }

    private ScanEntity saveScanEntity(String scanId, String traceId, String actorId,
                                      MessagingServiceEntity messagingServiceEntity, String emaId) {
        ScanEntity scan = ScanEntity.builder()
                .id(scanId)
                .traceId(traceId)
                .actorId(actorId)
                .messagingService(messagingServiceEntity)
                .emaId(emaId)
                .build();

        return save(scan);
    }

    private void setScanType(RouteBundle routeBundle, ScanEntity scanEntity) {


        ScanTypeEntity scanType = ScanTypeEntity.builder()
                .id(idGenerator.generateRandomUniqueId())
                .name(routeBundle.getScanType())
                .scan(scanEntity)
                .build();

        scanTypeRepository.save(scanType);

        setScanStatus(scanType);
    }

    private void setScanStatus(ScanTypeEntity scanType) {
        ScanStatusEntity scanStatus = ScanStatusEntity.builder()
                .id(idGenerator.generateRandomUniqueId())
                .status(ScanStatus.INITIATED.name())
                .scanType(scanType)
                .build();

        scanStatusRepository.save(scanStatus);

    }

    protected void setupRecipientsForScan(ScanEntity scanEntity, RouteBundle routeBundle) {

        for (RouteBundle recipient : routeBundle.getRecipients()) {
            RouteEntity route = routeService.findById(recipient.getRouteId())
                    .orElseThrow();
            updateScan(route, recipient, scanEntity);
        }
    }

    public Optional<ScanEntity> findById(String scanId) {
        return repository.findById(scanId);
    }

    /**
     * Sends the initial scan  status message to signal the start of Messaging Service scan.
     *
     * @param scanId             The Scan ID to set.
     * @param groupId            Not used at the moment. This will be the grouped Scan IDs.
     * @param messagingServiceId The ID of the Messaging Service being scanned.
     * @param scanTypes          The scan types included in the scan request.
     * @param status             The status of scan.
     */
    public void sendScanStatus(String orgId,
                               String groupId,
                               String scanId,
                               String traceId,
                               String actorId,
                               String messagingServiceId,
                               String scanTypes,
                               ScanStatus status) {
        producerTemplate.send("direct:overallScanStatusPublisher?block=false&failIfNoConsumers=false", exchange -> {
            exchange.getIn().setHeader(RouteConstants.SCHEDULE_ID, groupId);
            exchange.getIn().setHeader(RouteConstants.SCAN_ID, scanId);
            exchange.getIn().setHeader(RouteConstants.TRACE_ID, traceId);
            exchange.getIn().setHeader(RouteConstants.ACTOR_ID, actorId);
            exchange.getIn().setHeader(RouteConstants.ORG_ID, orgId);
            exchange.getIn().setHeader(RouteConstants.MESSAGING_SERVICE_ID, messagingServiceId);
            exchange.getIn().setHeader(RouteConstants.SCAN_TYPE, scanTypes);
            exchange.getIn().setHeader(RouteConstants.SCAN_STATUS, status);
            exchange.getIn().setHeader(RouteConstants.SCAN_STATUS_DESC, "");
        });
        meterRegistry.counter(MAAS_EMA_SCAN_EVENT_SENT, STATUS_TAG, status.name(), SCAN_ID_TAG, scanId).increment();
    }

    protected CompletableFuture<Exchange> scanAsync(String orgId,
                                                    String groupId,
                                                    String scanId,
                                                    String traceId,
                                                    String actorId,
                                                    RouteEntity route,
                                                    String messagingServiceId) {
        return producerTemplate.asyncSend("seda:" + route.getId(), exchange -> {
            // Need to set headers to let the Route have access to the Scan ID, Group ID, and Messaging Service ID.
            exchange.getIn().setHeader(RouteConstants.SCHEDULE_ID, groupId);
            exchange.getIn().setHeader(RouteConstants.SCAN_ID, scanId);
            exchange.getIn().setHeader(RouteConstants.TRACE_ID, traceId);
            exchange.getIn().setHeader(RouteConstants.ORG_ID, orgId);
            exchange.getIn().setHeader(RouteConstants.ACTOR_ID, actorId);
            exchange.getIn().setHeader(RouteConstants.MESSAGING_SERVICE_ID, messagingServiceId);
            exchange.getIn().setHeader(RouteConstants.SCAN_STATUS_DESC, "");

            exchange.getIn().setHeader(SchedulerConstants.SCHEDULER_TERMINATION_TIMER, true);
            exchange.getIn().setHeader(SchedulerConstants.SCHEDULER_TYPE, SchedulerType.INTERVAL.name());
            exchange.getIn().setHeader(SchedulerConstants.SCHEDULER_DESTINATION, "seda:terminateAsyncProcess");
            exchange.getIn().setHeader(SchedulerConstants.SCHEDULER_START_DELAY, 5000);
            exchange.getIn().setHeader(SchedulerConstants.SCHEDULER_INTERVAL, 5000);
            exchange.getIn().setHeader(SchedulerConstants.SCHEDULER_REPEAT_COUNT, 0);

            MDC.put(RouteConstants.SCHEDULE_ID, groupId);
            MDC.put(RouteConstants.SCAN_ID, scanId);
            MDC.put(RouteConstants.TRACE_ID, traceId);
            MDC.put(RouteConstants.ACTOR_ID, actorId);
            MDC.put(RouteConstants.ORG_ID, orgId);
            MDC.put(RouteConstants.MESSAGING_SERVICE_ID, messagingServiceId);

        }).whenComplete((exchange, exception) -> {
            if (exception != null) {
                log.error("Exception occurred while executing route {} for scan request: {}.", route.getId(), scanId, exception);
            } else {
                log.trace("Camel route {} for scan request: {} has been executed successfully.", route.getId(), scanId);
            }
            routeService.stopRoute(route);
        });
    }

    /**
     * Saves information pertaining to a Messaging Service scan.
     *
     * @param scanEntity The information of the Messaging Service scan.
     * @return The saved Messaging Service scan details.
     */
    public ScanEntity save(ScanEntity scanEntity) {
        return repository.save(scanEntity);
    }

    protected ScanRecipientHierarchyEntity save(RouteBundleHierarchyStore routeBundleHierarchy, String scanId) {
        ScanRecipientHierarchyEntity scanRecipientHierarchyEntity =
                ScanRecipientHierarchyEntity.builder()
                        .store(routeBundleHierarchy.getStore())
                        .scanId(scanId)
                        .build();

        return scanRecipientHierarchyRepository.save(scanRecipientHierarchyEntity);
    }

    protected List<String> parseRouteBundle(List<RouteBundle> routeBundles, List<String> scanTypes) {
        for (RouteBundle routeBundle : routeBundles) {
            scanTypes.add(routeBundle.getScanType());
            if (routeBundle.getRecipients() != null && !routeBundle.getRecipients().isEmpty()) {
                parseRouteBundle(routeBundle.getRecipients(), scanTypes);
            }
        }
        return scanTypes;
    }

    public List<ScanItemBO> listScans() {
        return StreamSupport.stream(repository.findAll().spliterator(), false)
                .map(se -> ScanItemBO.builder()
                        .id(se.getId())
                        .createdAt(se.getCreatedAt())
                        .messagingServiceId(se.getMessagingService().getId())
                        .messagingServiceName(se.getMessagingService().getName())
                        .messagingServiceType(se.getMessagingService().getType())
                        .build())
                .collect(Collectors.toUnmodifiableList());
    }

    public Page<ScanItemBO> findAll(Pageable pageable) {
        return repository.findAll(pageable)
                .map(this::mapToScanItemBO);
    }

    public Page<ScanItemBO> findByMessagingServiceId(String messagingServiceId, Pageable pageable) {
        return repository.findAllByMessagingServiceId(messagingServiceId, pageable)
                .map(this::mapToScanItemBO);
    }

    private ScanItemBO mapToScanItemBO(ScanEntity scanEntity) {
        List<ScanTypeBO> scanTypes = scanEntity.getScanTypes()
                .stream()
                .map(scanTypeEntity -> ScanTypeBO.builder()
                        .name(scanTypeEntity.getName())
                        .status(scanTypeEntity.getStatus().getStatus())
                        .build())
                .collect(Collectors.toUnmodifiableList());

        return ScanItemBO.builder()
                .id(scanEntity.getId())
                .createdAt(scanEntity.getCreatedAt())
                .messagingServiceId(scanEntity.getMessagingService().getId())
                .messagingServiceName(scanEntity.getMessagingService().getName())
                .messagingServiceType(scanEntity.getMessagingService().getType())
                .emaId(scanEntity.getEmaId())
                .scanTypes(scanTypes)
                .build();
    }

    protected RouteBundleHierarchyStore parseRouteRecipients(List<RouteBundle> routeBundles, RouteBundleHierarchyStore pathStore) {
        for (RouteBundle routeBundle : routeBundles) {
            if (routeBundle.getRecipients() != null && !routeBundle.getRecipients().isEmpty()) {
                pathStore = registerRouteRecipients(routeBundle, pathStore);
                parseRouteRecipients(routeBundle.getRecipients(), pathStore);
            }
        }
        return pathStore;
    }

    protected RouteBundleHierarchyStore registerRouteRecipients(RouteBundle routeBundle, RouteBundleHierarchyStore pathStore) {
        boolean parentExistsInStore = false;
        List<String> keys = new ArrayList<>(pathStore.getStore().keySet());
        String parentScanType = routeBundle.getScanType();

        List<String> recipients = routeBundle.getRecipients().stream()
                .map(RouteBundle::getScanType).collect(Collectors.toList());

        for (String key : keys) {
            String hierarchyPath = pathStore.getStore().get(key);
            String lastChild = StringUtils.substringAfterLast(hierarchyPath, ",");

            if (lastChild.equals(parentScanType)) {
                String firstRecipient = recipients.get(0);
                recipients.remove(0);
                String firstRecipientPath = pathStore.getStore().get(key) + "," + firstRecipient;
                pathStore.getStore().put(String.valueOf(key), firstRecipientPath);

                String basePath = pathStore.getStore().get(key).split(parentScanType, 2)[0];
                recipients.forEach(recipient -> {
                    String path = basePath + parentScanType + "," + recipient;
                    pathStore.getStore().put(String.valueOf(RouteBundleHierarchyStore.getStoreKey()), path);
                });
                parentExistsInStore = true;
                break;
            }
        }
        if (!parentExistsInStore) {
            recipients.forEach(recipient ->
                    pathStore.getStore().put(String.valueOf(RouteBundleHierarchyStore.getStoreKey()),
                            parentScanType + "," + recipient));
        }
        return pathStore;
    }

    public boolean isScanComplete(String scanId) {
        if (ObjectUtils.isEmpty(scanId)) {
            throw new IllegalArgumentException("Scan ID cannot be null or empty");
        }
        Set<String> completeScanStatuses = Set.of(
                ScanStatus.COMPLETE.name(),
                ScanStatus.FAILED.name(),
                ScanStatus.TIMED_OUT.name()
        );


        List<ScanTypeEntity> allScanTypes = scanTypeRepository.findAllByScanId(scanId);
        if (CollectionUtils.isEmpty(allScanTypes)) {
            return false;
        }
        return allScanTypes.stream()
                .map(scanStatusRepository::findByScanType)
                .allMatch(status -> completeScanStatuses.contains(status.getStatus()));

    }
}
