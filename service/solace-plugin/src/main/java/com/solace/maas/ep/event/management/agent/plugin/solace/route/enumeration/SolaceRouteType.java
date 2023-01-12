package com.solace.maas.ep.event.management.agent.plugin.solace.route.enumeration;

public enum SolaceRouteType {
    SOLACE_QUEUE_LISTING("queueListing"),
    SOLACE_QUEUE_CONFIG("queueConfiguration"),
    SOLACE_SUBSCRIPTION_CONFIG("subscriptionConfiguration");

    public final String label;

    SolaceRouteType(String label) {
        this.label = label;
    }
}
