package com.solace.maas.ep.event.management.agent.plugin.ibmmq.manager.client;

import com.solace.maas.ep.event.management.agent.plugin.ibmmq.client.http.IbmMqHttpClient;
import com.solace.maas.ep.event.management.agent.plugin.manager.client.MessagingServiceClientManager;
import com.solace.maas.ep.event.management.agent.plugin.messagingService.event.AuthenticationDetailsEvent;
import com.solace.maas.ep.event.management.agent.plugin.messagingService.event.ConnectionDetailsEvent;
import com.solace.maas.ep.event.management.agent.plugin.util.MessagingServiceConfigurationUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ExchangeFilterFunctions;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.support.WebClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import java.util.NoSuchElementException;

/**
 * Implementation of the IBM MQ Administration client.
 * <p>
 * This client provides access to the various administrative endpoints exposed by IBM MQ.
 */
@Slf4j
@Data
@Component
public class IbmMqClientManagerImpl implements MessagingServiceClientManager<IbmMqHttpClient> {

    public IbmMqClientManagerImpl() {
    }

    @Override
    public IbmMqHttpClient getClient(ConnectionDetailsEvent connectionDetailsEvent) {

        log.trace("Creating IBM MQ RESTful client for event broker [{}].",
                connectionDetailsEvent.getMessagingServiceId());

        //get authentication details from config file
        AuthenticationDetailsEvent authenticationDetailsEvent = connectionDetailsEvent.getAuthenticationDetails()
                .stream()
                .findFirst().orElseThrow(() -> {
                    String message = String.format("Could not find authentication details for service with id [%s].",
                            connectionDetailsEvent.getMessagingServiceId());
                    log.error(message);
                    return new NoSuchElementException(message);
                });

        //grab authentication details
        String username = MessagingServiceConfigurationUtil.getUsername(authenticationDetailsEvent);
        String password = MessagingServiceConfigurationUtil.getPassword(authenticationDetailsEvent);
        String url = connectionDetailsEvent.getUrl();

        //setup a basic webclient that will be used in generating
        //the actual IBM HTTP Client
        WebClient client = WebClient.builder()
                .filter(ExchangeFilterFunctions.basicAuthentication(username, password))
                .baseUrl(url)
                .build();

        HttpServiceProxyFactory proxyFactory = HttpServiceProxyFactory
                .builder(WebClientAdapter.forClient(client)).build();

        return proxyFactory.createClient(IbmMqHttpClient.class);
    }

}
