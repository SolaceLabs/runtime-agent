package com.solace.maas.ep.event.management.agent.plugin;

import com.solace.maas.ep.event.management.agent.plugin.jacoco.ExcludeFromJacocoGeneratedReport;
import com.solace.maas.ep.event.management.agent.plugin.route.RouteBundle;
import com.solace.maas.ep.event.management.agent.plugin.route.delegate.base.MessagingServiceRouteDelegateImpl;
import org.springframework.stereotype.Component;

import java.util.List;

@ExcludeFromJacocoGeneratedReport
@SuppressWarnings("CPD-START")
@Component
public class ScanLogsPublisherDelegateImpl extends MessagingServiceRouteDelegateImpl {
    public ScanLogsPublisherDelegateImpl() {
        super("SCAN_LOGS");
    }

    @Override
    public List<RouteBundle> generateRouteList(List<RouteBundle> destinations, List<RouteBundle> recipients,
                                               String scanType, String messagingServiceId) {
        return List.of(createRouteBundle(destinations, recipients, scanType, messagingServiceId,
                "seda:scanLogsPublisher", false));
    }
}
