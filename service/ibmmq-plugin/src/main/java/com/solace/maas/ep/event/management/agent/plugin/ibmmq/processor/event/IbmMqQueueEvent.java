package com.solace.maas.ep.event.management.agent.plugin.ibmmq.processor.event;

import lombok.Getter;

import java.io.Serializable;

/**
 * Attributes parsed from the response JSON.
 * Note: Other attributes may be returned in the raw JSON.
 */
@Getter
public class IbmMqQueueEvent implements Serializable {

    private static final long serialVersionUID = 7693606299215131178L;

    private String name;
    private String type;
}