package com.solace.maas.ep.event.management.agent.plugin.ibmmq.route.delegate;

import com.solace.maas.ep.event.management.agent.plugin.ibmmq.route.enumeration.IbmMqRouteId;
import com.solace.maas.ep.event.management.agent.plugin.ibmmq.route.enumeration.IbmMqRouteType;
import com.solace.maas.ep.event.management.agent.plugin.ibmmq.route.enumeration.IbmMqScanType;
import com.solace.maas.ep.event.management.agent.plugin.jacoco.ExcludeFromJacocoGeneratedReport;
import com.solace.maas.ep.event.management.agent.plugin.route.RouteBundle;
import com.solace.maas.ep.event.management.agent.plugin.route.delegate.base.MessagingServiceRouteDelegateImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@ExcludeFromJacocoGeneratedReport
@SuppressWarnings("CPD-START")
@Component
@Slf4j
public class IbmMqRouteDelegateImpl extends MessagingServiceRouteDelegateImpl {

    public IbmMqRouteDelegateImpl() {
        super("IBMMQ");
        log.debug("### PLUGIN `IBMMQ` HAS LOADED ###");
    }

    @Override
    public List<RouteBundle> generateRouteList(List<RouteBundle> destinations, List<RouteBundle> recipients,
                                               String scanType, String messagingServiceId) {
        List<RouteBundle> result = new ArrayList<>();

        switch (IbmMqScanType.valueOf(scanType)) {
            case IBMMQ_ALL:
                result.add(queueRouteBundle(destinations, recipients, messagingServiceId));
                result.add(subscriptionRouteBundle(destinations, recipients, messagingServiceId));
                break;
            case IBMMQ_QUEUE:
                result.add(queueRouteBundle(destinations, recipients, messagingServiceId));
                break;
            case IBMMQ_SUBSCRIPTION:
                result.add(subscriptionRouteBundle(destinations, recipients, messagingServiceId));
                break;
        }

        return result;
    }

    private RouteBundle queueRouteBundle(List<RouteBundle> destinations, List<RouteBundle> recipients,
                                         String messagingServiceId) {
        return createRouteBundle(destinations, recipients, IbmMqRouteType.IBMMQ_QUEUE.label, messagingServiceId,
                IbmMqRouteId.IBMMQ_QUEUE.label, true);
    }

    private RouteBundle subscriptionRouteBundle(List<RouteBundle> destinations, List<RouteBundle> recipients,
                                                String messagingServiceId) {
        return createRouteBundle(destinations, recipients, IbmMqRouteType.IBMMQ_SUBSCRIPTION.label, messagingServiceId,
                IbmMqRouteId.IBMMQ_SUBSCRIPTION.label, false);
    }
}
