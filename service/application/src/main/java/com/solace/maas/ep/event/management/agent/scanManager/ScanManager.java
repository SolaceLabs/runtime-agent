package com.solace.maas.ep.event.management.agent.scanManager;

import com.solace.maas.ep.common.messages.ScanCommandMessage;
import com.solace.maas.ep.common.messages.ScanStatusMessage;
import com.solace.maas.ep.common.model.ScanType;
import com.solace.maas.ep.event.management.agent.config.eventPortal.EventPortalProperties;
import com.solace.maas.ep.event.management.agent.plugin.constants.RouteConstants;
import com.solace.maas.ep.event.management.agent.plugin.constants.ScanStatus;
import com.solace.maas.ep.event.management.agent.plugin.manager.loader.PluginLoader;
import com.solace.maas.ep.event.management.agent.plugin.route.RouteBundle;
import com.solace.maas.ep.event.management.agent.plugin.route.handler.base.MessagingServiceRouteDelegate;
import com.solace.maas.ep.event.management.agent.publisher.ScanStatusPublisher;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.MessagingServiceEntity;
import com.solace.maas.ep.event.management.agent.scanManager.model.ScanItemBO;
import com.solace.maas.ep.event.management.agent.scanManager.model.ScanRequestBO;
import com.solace.maas.ep.event.management.agent.service.MessagingServiceDelegateServiceImpl;
import com.solace.maas.ep.event.management.agent.service.ScanService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;


@Slf4j
@Service
public class ScanManager {

    private final MessagingServiceDelegateServiceImpl messagingServiceDelegateService;
    private final ScanService scanService;
    private final String runtimeAgentId;
    // This is an optional dependency since it is not available in standalone mode.
    // If the bean is not present, the publisher will not be used.
    private final Optional<ScanStatusPublisher> scanStatusPublisherOpt;

    @Autowired
    public ScanManager(MessagingServiceDelegateServiceImpl messagingServiceDelegateService,
                       ScanService scanService,
                       EventPortalProperties eventPortalProperties,
                       Optional<ScanStatusPublisher> scanStatusPublisher) {
        this.messagingServiceDelegateService = messagingServiceDelegateService;
        this.scanService = scanService;
        this.scanStatusPublisherOpt = scanStatusPublisher;
        runtimeAgentId = eventPortalProperties.getRuntimeAgentId();
    }

    public String scan(ScanRequestBO scanRequestBO) {
        String messagingServiceId = scanRequestBO.getMessagingServiceId();
        String scanId = scanRequestBO.getScanId();
        String traceId = scanRequestBO.getTraceId();
        String actorId = scanRequestBO.getActorId();
        String groupId = UUID.randomUUID().toString();

        MDC.put(RouteConstants.SCAN_ID, scanId);
        MDC.put(RouteConstants.TRACE_ID, traceId);
        MDC.put(RouteConstants.ACTOR_ID, actorId);
        MDC.put(RouteConstants.SCHEDULE_ID, groupId);
        MDC.put(RouteConstants.ORG_ID, scanRequestBO.getOrgId());
        MDC.put(RouteConstants.MESSAGING_SERVICE_ID, messagingServiceId);

        MessagingServiceEntity messagingServiceEntity = retrieveMessagingServiceEntity(messagingServiceId);

        MessagingServiceRouteDelegate scanDelegate =
                PluginLoader.findPlugin(messagingServiceEntity.getType());

        Set<MessagingServiceEntity> messagingServiceEntitySet = messagingServiceDelegateService.getMessagingServicesRelations(messagingServiceId);
        messagingServiceEntitySet.add(messagingServiceEntity);
        Map<String, MessagingServiceRouteDelegate> delegates =
                messagingServiceEntitySet.stream()
                        .map(messagingServiceEntity1 ->
                                new AbstractMap.SimpleEntry<>(messagingServiceEntity1.getId(), PluginLoader.findPlugin(messagingServiceEntity1.getType())))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

        Objects.requireNonNull(scanDelegate,
                String.format("Unable to find messaging service plugin for plugin type %s. Valid types are %s.",
                        messagingServiceEntity.getType(),
                        String.join(", ", PluginLoader.getKeys())));

        List<String> scanDestinations = scanRequestBO.getDestinations();
        List<RouteBundle> destinations = scanDestinations.stream()
                .map(PluginLoader::findPlugin)
                .filter(Objects::nonNull)
                .map(delegate -> delegate.generateRouteList(
                                List.of(),
                                List.of(),
                                scanRequestBO.getScanTypes().stream().findFirst().orElseThrow(),
                                messagingServiceId
                        ).stream()
                        .findFirst()
                        .orElseThrow())
                .toList();

        List<String> brokerScanTypes = scanRequestBO.getScanTypes();
        List<RouteBundle> routes = brokerScanTypes.stream()
                .distinct()
                .flatMap(brokerScanType -> delegates.entrySet().stream()
                        .map(e -> e.getValue().generateRouteList(destinations, List.of(),
                                brokerScanType, e.getKey()))
                        .filter(Objects::nonNull)
                        .filter(list -> !list.isEmpty())
                        .toList().stream()
                ).toList().stream().flatMap(List::stream).toList();

        return scanService.singleScan(scanRequestBO.getOrgId(), routes, groupId, scanId, traceId, actorId, messagingServiceEntity, runtimeAgentId);
    }

    public void handleError(String orgId, ScanCommandMessage message) {

        if (scanStatusPublisherOpt.isEmpty()) {
            return;
        }
        ScanStatusPublisher scanStatusPublisher = scanStatusPublisherOpt.get();

        List<String> scanTypeNames = message.getScanTypes().stream().map(ScanType::name).toList();

        ScanStatusMessage response = new ScanStatusMessage(
                message.getOrgId(),
                message.getScanId(),
                MDC.get(RouteConstants.TRACE_ID),
                MDC.get(RouteConstants.ACTOR_ID),
                ScanStatus.FAILED.name(),
                "Scan failed",
                scanTypeNames
        );

        Map<String, String> topicVars = Map.of(
                "orgId", orgId,
                "runtimeAgentId", runtimeAgentId
        );
        scanStatusPublisher.sendOverallScanStatus(response, topicVars);
    }

    private MessagingServiceEntity retrieveMessagingServiceEntity(String messagingServiceId) {
        return messagingServiceDelegateService.getMessagingServiceById(messagingServiceId);
    }

    public List<ScanItemBO> listScans() {
        return scanService.listScans();
    }

    public Page<ScanItemBO> findAll(Pageable pageable) {
        return scanService.findAll(pageable);
    }

    public Page<ScanItemBO> findByMessagingServiceId(String messagingServiceId, Pageable pageable) {
        return scanService.findByMessagingServiceId(messagingServiceId, pageable);
    }


    public boolean isScanComplete(String scanId) {
        if (ObjectUtils.isEmpty(scanId)) {
            throw new IllegalArgumentException("Scan ID cannot be null or empty");
        }
        return scanService.isScanComplete(scanId);
    }
}
