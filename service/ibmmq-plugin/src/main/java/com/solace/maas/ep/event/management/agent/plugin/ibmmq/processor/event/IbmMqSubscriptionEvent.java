package com.solace.maas.ep.event.management.agent.plugin.ibmmq.processor.event;

import lombok.Data;

import java.io.Serializable;

/**
 * Attributes parsed from the response JSON.
 * Note: Other attributes may be returned in the raw JSON.
 */
@Data
public class IbmMqSubscriptionEvent implements Serializable {

    private static final long serialVersionUID = 7693606299215131178L;

    private String resolvedTopicString;
    private String name;
    private String id;
}