package com.solace.maas.ep.event.management.agent.plugin.ibmmq.client.http;

import com.solace.maas.ep.event.management.agent.plugin.ibmmq.processor.event.IbmMqSubscriptionEvent;
import lombok.Data;

import java.util.List;

/**
 * Represents the response JSON from the IBM MQ Admin client
 * when querying for Subscription (topic) information.
 */
@Data
public class IbmMqSubscriptionResponse {

    List<IbmMqSubscriptionEvent> subscription;
}
