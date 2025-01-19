package com.solace.maas.ep.event.management.agent.plugin.ibmmq.client.http;

import com.solace.maas.ep.event.management.agent.plugin.ibmmq.processor.event.IbmMqQueueEvent;
import lombok.Getter;

import java.util.List;

/**
 * Represents the response JSON from the IBM MQ Admin client
 * when querying for Queue information.
 */
@Getter
public class IbmMqQueueResponse {

    private List<IbmMqQueueEvent> queue;
}
