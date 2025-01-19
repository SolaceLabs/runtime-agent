package com.solace.maas.ep.event.management.agent.plugin.ibmmq.route.delegate;

import com.solace.maas.ep.event.management.agent.plugin.ibmmq.IbmMqTestConfig;
import com.solace.maas.ep.event.management.agent.plugin.ibmmq.route.enumeration.IbmMqScanType;
import com.solace.maas.ep.event.management.agent.plugin.route.RouteBundle;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

@ActiveProfiles("TEST")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = IbmMqTestConfig.class)
public class IbmMqRouteDelegateImplTests {

    @InjectMocks
    private IbmMqRouteDelegateImpl ibmMqRouteDelegate;

    private final List<RouteBundle> destinations = List.of(
            RouteBundle.builder()
                    .destinations(List.of())
                    .recipients(List.of())
                    .routeId("testRoute")
                    .firstRouteInChain(false)
                    .messagingServiceId("service1")
                    .build());

    @Test
    public void testGenerateIbmMqQueueRouteList() {
        List<RouteBundle> routeBundles = ibmMqRouteDelegate.generateRouteList(destinations, List.of(),
                IbmMqScanType.IBMMQ_QUEUE.name(),
                "service1");

        assertThatNoException();
        assertThat(!routeBundles.isEmpty());
    }

    @Test
    public void testGenerateIbmMqSubscriptionRouteList() {
        List<RouteBundle> routeBundles = ibmMqRouteDelegate.generateRouteList(destinations, List.of(),
                IbmMqScanType.IBMMQ_SUBSCRIPTION.name(),
                "service1");

        assertThatNoException();
        assertThat(!routeBundles.isEmpty());
    }

    @Test
    public void testGenerateIbmMqAllRouteList() {
        List<RouteBundle> routeBundles = ibmMqRouteDelegate.generateRouteList(destinations, List.of(),
                IbmMqScanType.IBMMQ_ALL.name(),
                "service1");

        assertThatNoException();
        assertThat(!routeBundles.isEmpty());
    }
}
