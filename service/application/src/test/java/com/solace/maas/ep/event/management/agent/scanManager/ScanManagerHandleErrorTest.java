package com.solace.maas.ep.event.management.agent.scanManager;

import com.solace.maas.ep.common.messages.ScanCommandMessage;
import com.solace.maas.ep.common.model.ScanDestination;
import com.solace.maas.ep.common.model.ScanType;
import com.solace.maas.ep.event.management.agent.TestConfig;
import com.solace.maas.ep.event.management.agent.config.eventPortal.EventPortalProperties;
import com.solace.maas.ep.event.management.agent.publisher.ScanStatusPublisher;
import com.solace.maas.ep.event.management.agent.service.MessagingServiceDelegateServiceImpl;
import com.solace.maas.ep.event.management.agent.service.ScanService;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ActiveProfiles("TEST")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = TestConfig.class)
class ScanManagerHandleErrorTest {

    @Mock
    EventPortalProperties eventPortalProperties;

    @Mock
    MessagingServiceDelegateServiceImpl messagingServiceDelegateService;

    @Mock
    private ScanService scanService;

    @Mock
    private ScanStatusPublisher scanStatusPublisher;

    @Test
    void testScanManagerConnectedHandleError(){
        when(eventPortalProperties.getOrganizationId()).thenReturn("orgId");
        when(eventPortalProperties.getRuntimeAgentId()).thenReturn("runtimeAgentId");


        ScanManager scanManagerUnderTest = new ScanManager(
                messagingServiceDelegateService,
                scanService,
                eventPortalProperties,
                Optional.of(scanStatusPublisher)
        );
        scanManagerUnderTest.handleError("orgId",createScanCommandMessage());
        verify(scanStatusPublisher, times(1)).sendOverallScanStatus(any(),any());
    }

    @Test
    void testScanManagerStandaloneHandleError(){
        when(eventPortalProperties.getOrganizationId()).thenReturn("orgId");
        when(eventPortalProperties.getRuntimeAgentId()).thenReturn("runtimeAgentId");

        ScanManager scanManagerUnderTest = new ScanManager(
                messagingServiceDelegateService,
                scanService,
                eventPortalProperties,
                Optional.empty()
        );
        // should just do "nothing" and not throw an exception when scanStatusPublisher is not present
        assertDoesNotThrow(() ->
            scanManagerUnderTest.handleError("orgId", createScanCommandMessage()));

    }

    private ScanCommandMessage createScanCommandMessage(){
        return new ScanCommandMessage(
                "messageServiceId",
                "scanId",
                List.of(ScanType.SOLACE_ALL),
                List.of(ScanDestination.EVENT_PORTAL),
                null);
    }
}
