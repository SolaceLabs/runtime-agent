package com.solace.maas.ep.event.management.agent.scanManager.rest;

import com.solace.maas.ep.common.model.ScanRequestDTO;
import com.solace.maas.ep.event.management.agent.TestConfig;
import com.solace.maas.ep.event.management.agent.config.eventPortal.EventPortalProperties;
import com.solace.maas.ep.event.management.agent.config.eventPortal.GatewayMessagingProperties;
import com.solace.maas.ep.event.management.agent.config.eventPortal.GatewayProperties;
import com.solace.maas.ep.event.management.agent.plugin.route.exceptions.ClientException;
import com.solace.maas.ep.event.management.agent.scanManager.ScanManager;
import com.solace.maas.ep.event.management.agent.scanManager.mapper.ScanRequestMapper;
import com.solace.maas.ep.event.management.agent.scanManager.model.ScanRequestBO;
import com.solace.maas.ep.event.management.agent.util.IDGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ActiveProfiles("TEST")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = TestConfig.class)
public class EMAControllerTest {

    @Autowired
    private ScanRequestMapper scanRequestMapper;


    @Test
    public void testEMAControllerInConnectedMode() {
        ScanManager scanManager = mock(ScanManager.class);
        IDGenerator idGenerator = mock(IDGenerator.class);
        EventPortalProperties eventPortalProperties = mock(EventPortalProperties.class);

        ScanRequestDTO scanRequestDTO = new ScanRequestDTO(List.of("topics"), List.of());
        ScanRequestBO scanRequestBO = new ScanRequestBO(
                "orgId",
                "id",
                "scanConnected",
                "traceId",
                "actorId",
                List.of("TEST_SCAN_TYPE"),
                List.of());

        when(eventPortalProperties.getGateway())
                .thenReturn(GatewayProperties.builder()
                        .messaging(GatewayMessagingProperties.builder().standalone(false).build())
                        .build());

        when(scanManager.scan(scanRequestBO))
                .thenReturn("scanId");

        EMAController controller = new EMAControllerImpl(scanRequestMapper, scanManager, idGenerator, eventPortalProperties);

        // Scan requests via REST endpoints are prevented in connected mode, e.g., standalone=false
        ClientException thrown = assertThrows(ClientException.class, () -> controller.scan("id", scanRequestDTO));

        assertTrue(thrown.getMessage().contains("Scan requests via REST endpoint could not be initiated in connected mode."));
    }

    @Test
    public void testEMAControllerInStandAloneModeWithEventPortalDestination() {
        ScanManager scanManager = mock(ScanManager.class);
        IDGenerator idGenerator = mock(IDGenerator.class);
        EventPortalProperties eventPortalProperties = mock(EventPortalProperties.class);

        ScanRequestDTO scanRequestDTO = new ScanRequestDTO(List.of("topics"), List.of("EVENT_PORTAL"));
        ScanRequestBO scanRequestBO = new ScanRequestBO(
                "orgId",
                "id",
                "scanId",
                "traceId",
                "actorId",
                List.of("TEST_SCAN"),
                List.of("EVENT_PORTAL"));

        when(eventPortalProperties.getGateway())
                .thenReturn(GatewayProperties.builder()
                        .messaging(GatewayMessagingProperties.builder().standalone(true).build())
                        .build());

        when(scanManager.scan(scanRequestBO))
                .thenReturn("scanId");

        EMAController controller = new EMAControllerImpl(scanRequestMapper, scanManager, idGenerator, eventPortalProperties);

        ClientException thrown = assertThrows(ClientException.class, () -> controller.scan("id", scanRequestDTO));

        assertThat(thrown.getMessage()).contains("Scan data could not be streamed to the Event Portal in standalone mode.");
    }

    @Test
    public void testEMAControllerInStandAloneMode() {
        ScanManager scanManager = mock(ScanManager.class);
        IDGenerator idGenerator = mock(IDGenerator.class);
        EventPortalProperties eventPortalProperties = mock(EventPortalProperties.class);

        ScanRequestDTO scanRequestDTO = new ScanRequestDTO(List.of("topics"), List.of());
        ScanRequestBO scanRequestBO = new ScanRequestBO(
                "orgId",
                "id",
                "scanId",
                "traceId",
                "actorId",
                List.of("TEST_SCAN"),
                List.of());

        when(eventPortalProperties.getGateway())
                .thenReturn(GatewayProperties.builder()
                        .messaging(GatewayMessagingProperties.builder().standalone(true).build())
                        .build());

        when(scanManager.scan(scanRequestBO))
                .thenReturn("scanId");

        EMAController controller = new EMAControllerImpl(scanRequestMapper, scanManager, idGenerator, eventPortalProperties);

        ResponseEntity<String> reply =
                controller.scan("id", scanRequestDTO);

        assertThat(reply.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(reply.getBody()).contains("Scan started.");

        assertThatNoException();
    }
}
