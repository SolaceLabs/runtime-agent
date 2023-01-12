package com.solace.maas.ep.event.management.agent.service;

import com.solace.maas.ep.event.management.agent.event.MessagingServiceEvent;
import com.solace.maas.ep.event.management.agent.plugin.messagingService.event.AuthenticationDetailsEvent;
import com.solace.maas.ep.event.management.agent.plugin.messagingService.event.ConnectionDetailsEvent;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.AuthenticationDetailsEntity;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.AuthenticationPropertiesEntity;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.ConnectionDetailsEntity;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.ConnectionPropertiesEntity;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.CredentialDetailsEntity;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.CredentialOperationsEntity;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.CredentialPropertiesEntity;
import com.solace.maas.ep.event.management.agent.repository.model.mesagingservice.MessagingServiceEntity;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class MessagingServiceEventToEntityConverter extends MessagingServiceConverter {
    public MessagingServiceEntity convert(MessagingServiceEvent messagingServiceEvent) {
        MessagingServiceEntity messagingServiceEntity = MessagingServiceEntity.builder()
                .id(messagingServiceEvent.getId())
                .type(messagingServiceEvent.getMessagingServiceType())
                .name(messagingServiceEvent.getName())
                .build();

        List<ConnectionDetailsEntity> connections = convertConnectionDetails(messagingServiceEntity, messagingServiceEvent);

        messagingServiceEntity.setConnections(connections);
        return messagingServiceEntity;
    }

    private List<ConnectionDetailsEntity> convertConnectionDetails(MessagingServiceEntity messagingServiceEntity,
                                                                   MessagingServiceEvent messagingServiceEvent) {
        return ensureList(messagingServiceEvent.getConnectionDetails()).stream()
                .map(connEvent -> {
                    ConnectionDetailsEntity connection = ConnectionDetailsEntity.builder()
                            .id(connEvent.getId())
                            .messagingService(messagingServiceEntity)
                            .name(connEvent.getName())
                            .url(connEvent.getUrl())
                            .build();

                    connection.setProperties(ensureList(connEvent.getProperties()).stream()
                            .map(prop ->
                                    ConnectionPropertiesEntity.builder()
                                            .id(prop.getId())
                                            .name(prop.getName())
                                            .value(prop.getValue())
                                            .connectionDetails(connection)
                                            .build())
                            .collect(Collectors.toUnmodifiableList()));

                    List<AuthenticationDetailsEntity> authenticationDetailsEntities =
                            convertAuthenticationDetailsEntities(connection, connEvent);

                    connection.setAuthentication(authenticationDetailsEntities);
                    return connection;
                }).collect(Collectors.toUnmodifiableList());
    }

    private List<AuthenticationDetailsEntity> convertAuthenticationDetailsEntities(ConnectionDetailsEntity connection,
                                                                                   ConnectionDetailsEvent connEvent) {
        return ensureList(connEvent.getAuthenticationDetails()).stream()
                .map(authEvent -> {
                    AuthenticationDetailsEntity auth = AuthenticationDetailsEntity.builder()
                            .id(authEvent.getId())
                            .connections(connection)
                            .protocol(authEvent.getProtocol())
                            .build();

                    List<AuthenticationPropertiesEntity> authenticationPropertiesEntities =
                            ensureList(authEvent.getProperties()).stream()
                                    .map(prop ->
                                            AuthenticationPropertiesEntity.builder()
                                                    .id(prop.getId())
                                                    .name(prop.getName())
                                                    .value(prop.getValue())
                                                    .authentication(auth)
                                                    .build())
                                    .collect(Collectors.toUnmodifiableList());

                    List<CredentialDetailsEntity> credentialDetailsEntities = convertCredentials(auth, authEvent);

                    auth.setCredentials(credentialDetailsEntities);
                    auth.setProperties(authenticationPropertiesEntities);
                    return auth;
                }).collect(Collectors.toUnmodifiableList());
    }

    private List<CredentialDetailsEntity> convertCredentials(AuthenticationDetailsEntity auth,
                                                             AuthenticationDetailsEvent authEvent) {
        return ensureList(authEvent.getCredentials()).stream()
                .map(credEvent -> {
                    CredentialDetailsEntity credentialDetails = CredentialDetailsEntity.builder()
                            .id(credEvent.getId())
                            .authentication(auth)
                            .source(credEvent.getSource())
                            .build();

                    List<CredentialPropertiesEntity> credentialProperties =
                            ensureList(credEvent.getProperties()).stream()
                                    .map(prop -> CredentialPropertiesEntity.builder()
                                            .id(prop.getId())
                                            .name(prop.getName())
                                            .value(prop.getValue())
                                            .credentials(credentialDetails)
                                            .build())
                                    .collect(Collectors.toUnmodifiableList());

                    List<CredentialOperationsEntity> operationsEntities =
                            ensureList(credEvent.getOperations()).stream()
                                    .map(op -> CredentialOperationsEntity.builder()
                                            .id(op.getId())
                                            .credentials(credentialDetails)
                                            .name(op.getName())
                                            .build())
                                    .collect(Collectors.toUnmodifiableList());

                    credentialDetails.setProperties(credentialProperties);
                    credentialDetails.setOperations(operationsEntities);

                    return credentialDetails;
                }).collect(Collectors.toUnmodifiableList());
    }
}
