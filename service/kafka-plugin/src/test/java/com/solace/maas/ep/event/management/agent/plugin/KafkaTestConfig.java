package com.solace.maas.ep.event.management.agent.plugin;

import com.solace.maas.ep.event.management.agent.plugin.processor.EmptyScanEntityProcessor;
import com.solace.maas.ep.event.management.agent.plugin.processor.logging.MDCProcessor;
import com.solace.maas.ep.event.management.agent.plugin.route.manager.RouteManager;
import com.solace.maas.ep.event.management.agent.plugin.service.MessagingServiceDelegateService;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import static org.mockito.Mockito.mock;


@TestConfiguration
public class KafkaTestConfig {
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
    public EmptyScanEntityProcessor emptyScanEntityProcessor() {
        return mock(EmptyScanEntityProcessor.class);
    }

    @Bean
    @Primary
    public RouteManager RouteManager() {
        return mock(RouteManager.class);
    }
}
