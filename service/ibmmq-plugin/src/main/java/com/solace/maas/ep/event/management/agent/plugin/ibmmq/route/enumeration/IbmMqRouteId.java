package com.solace.maas.ep.event.management.agent.plugin.ibmmq.route.enumeration;

public enum IbmMqRouteId {
    IBMMQ_QUEUE("IBMMQQueueListing"),
    IBMMQ_SUBSCRIPTION("IBMMQSubscriptionListing");

    public final String label;

    IbmMqRouteId(String label) {
        this.label = label;
    }
}
