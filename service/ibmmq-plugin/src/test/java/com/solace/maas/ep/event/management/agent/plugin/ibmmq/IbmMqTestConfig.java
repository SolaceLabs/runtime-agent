package com.solace.maas.ep.event.management.agent.plugin.ibmmq;

import com.solace.maas.ep.event.management.agent.plugin.processor.ScanTypeDescendentsProcessor;
import com.solace.maas.ep.event.management.agent.plugin.processor.logging.MDCProcessor;
import com.solace.maas.ep.event.management.agent.plugin.route.manager.RouteManager;
import com.solace.maas.ep.event.management.agent.plugin.service.MessagingServiceDelegateService;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import static org.mockito.Mockito.mock;

@TestConfiguration
public class IbmMqTestConfig {

    @Bean
    @Primary
    public MessagingServiceDelegateService messagingServiceDelegateService() {
        return mock(MessagingServiceDelegateService.class);
    }

    @Bean
    @Primary
    public MDCProcessor mdcProcessor() {
        return mock(MDCProcessor.class);
    }

    @Bean
    @Primary
    public ScanTypeDescendentsProcessor emptyScanEntityProcessor() {
        return mock(ScanTypeDescendentsProcessor.class);
    }

    @Bean
    @Primary
    public RouteManager RouteManager() {
        return mock(RouteManager.class);
    }

}
