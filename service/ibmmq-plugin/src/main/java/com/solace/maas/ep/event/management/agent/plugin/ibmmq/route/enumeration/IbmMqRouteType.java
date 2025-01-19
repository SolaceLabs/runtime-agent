package com.solace.maas.ep.event.management.agent.plugin.ibmmq.route.enumeration;

public enum IbmMqRouteType {
    IBMMQ_QUEUE("queueListing"),
    IBMMQ_SUBSCRIPTION("subscriptionListing");

    public final String label;

    IbmMqRouteType(String label) {
        this.label = label;
    }
}
