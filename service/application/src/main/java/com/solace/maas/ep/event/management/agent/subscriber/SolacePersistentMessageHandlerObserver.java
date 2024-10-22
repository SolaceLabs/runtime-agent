package com.solace.maas.ep.event.management.agent.subscriber;

import com.solace.messaging.receiver.InboundMessage;

/**
 * The SolacePersistentMessageHandlerObserver interface defines methods to observe
 * the lifecycle of messages handled by a Solace message handler. Implementers can
 * use this interface to react to various stages of message processing.
 * Primary use case is for testing purposes.
 */
public interface SolacePersistentMessageHandlerObserver {

     void onMessageReceived(InboundMessage message);
     void onMessageProcessingInitiated(InboundMessage message);
     void onMessageProcessingCompleted(InboundMessage message);
     void onMessageProcessingAcknowledged(InboundMessage message);
     void onMessageProcessingFailed(InboundMessage message);

}
