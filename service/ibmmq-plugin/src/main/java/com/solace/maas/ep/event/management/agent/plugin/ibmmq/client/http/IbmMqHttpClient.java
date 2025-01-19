package com.solace.maas.ep.event.management.agent.plugin.ibmmq.client.http;

import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;

/**
 * Interface for Spring HTTPInterface to create the IBM MQ Admin Client.
 */
@HttpExchange
public interface IbmMqHttpClient {

    /*Gets all queues on a given queue manager*/
    @GetExchange("/queue")
    IbmMqQueueResponse getQueues();

    /*Gets all subscriptions (topics) on a given queue manager*/
    @GetExchange("/subscription")
    IbmMqSubscriptionResponse getSubscriptions();
}
