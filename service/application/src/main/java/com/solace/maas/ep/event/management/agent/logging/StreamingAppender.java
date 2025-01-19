package com.solace.maas.ep.event.management.agent.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.solace.maas.ep.event.management.agent.plugin.constants.RouteConstants;
import com.solace.maas.ep.event.management.agent.plugin.manager.loader.PluginLoader;
import com.solace.maas.ep.event.management.agent.plugin.route.RouteBundle;
import com.solace.maas.ep.event.management.agent.plugin.route.handler.base.MessagingServiceRouteDelegate;
import com.solace.maas.ep.event.management.agent.repository.model.route.RouteEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Slf4j
@Data
public class StreamingAppender extends AppenderBase<ILoggingEvent> {

    protected ProducerTemplate producerTemplate;
    protected boolean standalone;

    @Override
    protected void append(ILoggingEvent event) {
        if (!standalone) {
            if (StringUtils.isNotEmpty(event.getMDCPropertyMap().get(RouteConstants.SCAN_ID))) {
                sendScanLogsAsync(
                        event.getMDCPropertyMap().get(RouteConstants.ORG_ID),
                        event,
                        event.getMDCPropertyMap().get(RouteConstants.SCAN_ID),
                        event.getMDCPropertyMap().get(RouteConstants.TRACE_ID),
                        event.getMDCPropertyMap().get(RouteConstants.ACTOR_ID),
                        event.getMDCPropertyMap().get(RouteConstants.SCAN_TYPE),
                        event.getMDCPropertyMap().get(RouteConstants.SCHEDULE_ID),
                        event.getMDCPropertyMap().get(RouteConstants.MESSAGING_SERVICE_ID));
            } else if (StringUtils.isNotEmpty(event.getMDCPropertyMap().get(RouteConstants.COMMAND_CORRELATION_ID))) {
                sendCommandLogsAsync(
                        event.getMDCPropertyMap().get(RouteConstants.ORG_ID),
                        event,
                        event.getMDCPropertyMap().get(RouteConstants.COMMAND_CORRELATION_ID),
                        event.getMDCPropertyMap().get(RouteConstants.TRACE_ID),
                        event.getMDCPropertyMap().get(RouteConstants.ACTOR_ID),
                        event.getMDCPropertyMap().get(RouteConstants.MESSAGING_SERVICE_ID));
            }
        }
    }

    private void sendCommandLogsAsync(String orgId,
                                      ILoggingEvent event, String commandCorrelationId, String traceId,
                                      String actorId, String messagingServiceId) {


        RouteEntity route = RouteEntity.builder()
                .id("seda:commandLogsPublisher")
                .active(true)
                .build();

        producerTemplate.asyncSend(route.getId(), exchange -> {
            // Need to set headers to let the Route have access to the Correlation ID, Group ID, and Messaging Service ID.
            exchange.getIn().setHeader(RouteConstants.COMMAND_CORRELATION_ID, commandCorrelationId);
            exchange.getIn().setHeader(RouteConstants.TRACE_ID, traceId);
            exchange.getIn().setHeader(RouteConstants.ORG_ID, orgId);
            exchange.getIn().setHeader(RouteConstants.ACTOR_ID, actorId);
            exchange.getIn().setHeader(RouteConstants.MESSAGING_SERVICE_ID, messagingServiceId);

            exchange.getIn().setBody(event);

            MDC.clear();
        }).whenComplete((exchange, exception) -> {
            if (exception != null) {
                log.error("Exception occurred while executing route publishLogs for commandCorrelation {}.", commandCorrelationId, exception);
            }
        });
    }

    private void sendScanLogsAsync(String orgId, ILoggingEvent event, String scanId, String traceId, String actorId,
                                   String scanType, String groupId, String messagingServiceId) {
        RouteEntity route = creatLoggingRoute(scanType, messagingServiceId);

        producerTemplate.asyncSend(route.getId(), exchange -> {
            // Need to set headers to let the Route have access to the Scan ID, Group ID, and Messaging Service ID.
            exchange.getIn().setHeader(RouteConstants.SCAN_ID, scanId);
            exchange.getIn().setHeader(RouteConstants.TRACE_ID, traceId);
            exchange.getIn().setHeader(RouteConstants.ACTOR_ID, actorId);
            exchange.getIn().setHeader(RouteConstants.SCAN_TYPE, scanType);
            exchange.getIn().setHeader(RouteConstants.ORG_ID, orgId);
            exchange.getIn().setHeader(RouteConstants.SCHEDULE_ID, groupId);
            exchange.getIn().setHeader(RouteConstants.MESSAGING_SERVICE_ID, messagingServiceId);

            exchange.getIn().setBody(event);

            MDC.clear();
        }).whenComplete((exchange, exception) -> {
            if (exception != null) {
                log.error("Exception occurred while executing route publishLogs for scan {}.", scanId, exception);
            }
        });
    }

    private RouteEntity creatLoggingRoute(String scanType, String messagingServiceId) {
        MessagingServiceRouteDelegate scanDelegate =
                PluginLoader.findPlugin("SCAN_LOGS");

        List<RouteBundle> routes = scanDelegate.generateRouteList(
                List.of(),
                List.of(),
                scanType,
                messagingServiceId);

        return RouteEntity.builder()
                .id(routes.stream().findFirst().orElseThrow().getRouteId())
                .active(true)
                .build();
    }
}
