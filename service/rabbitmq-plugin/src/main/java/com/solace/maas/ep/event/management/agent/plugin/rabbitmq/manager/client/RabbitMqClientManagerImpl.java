package com.solace.maas.ep.event.management.agent.plugin.rabbitmq.manager.client;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.ClientParameters;
import com.solace.maas.ep.event.management.agent.plugin.kafka.manager.client.MessagingServiceClientManager;
import com.solace.maas.ep.event.management.agent.plugin.messagingService.event.AuthenticationDetailsEvent;
import com.solace.maas.ep.event.management.agent.plugin.messagingService.event.ConnectionDetailsEvent;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;

@Slf4j
@Data
public class RabbitMqClientManagerImpl implements MessagingServiceClientManager<Client> {
    @Override
    public Client getClient(ConnectionDetailsEvent connectionDetailsEvent) {
        AuthenticationDetailsEvent authenticationDetailsEvent =
                connectionDetailsEvent.getAuthenticationDetails().stream().findFirst()
                        .orElseThrow(() -> {
                            String message = String.format("Could not find authentication details for service with id [%s].",
                                    connectionDetailsEvent.getMessagingServiceId());
                            log.error(message);
                            return new NoSuchElementException(message);
                        });

        try {
            return new Client(
                    new ClientParameters()
                            .url(connectionDetailsEvent.getConnectionUrl())
                            .username(authenticationDetailsEvent.getUsername())
                            .password(authenticationDetailsEvent.getPassword())
            );
        } catch (URISyntaxException | MalformedURLException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }
}
