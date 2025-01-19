package com.solace.maas.ep.event.management.agent.plugin.ibmmq.processor;

import com.solace.maas.ep.event.management.agent.plugin.constants.RouteConstants;
import com.solace.maas.ep.event.management.agent.plugin.ibmmq.client.http.IbmMqHttpClient;
import com.solace.maas.ep.event.management.agent.plugin.ibmmq.client.http.IbmMqSubscriptionResponse;
import com.solace.maas.ep.event.management.agent.plugin.ibmmq.processor.event.IbmMqSubscriptionEvent;
import com.solace.maas.ep.event.management.agent.plugin.processor.base.ResultProcessorImpl;
import com.solace.maas.ep.event.management.agent.plugin.service.MessagingServiceDelegateService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class IbmMqSubscriptionProcessor extends ResultProcessorImpl<List<IbmMqSubscriptionEvent>, Void> {
    private final MessagingServiceDelegateService messagingServiceDelegateService;

    @Autowired
    public IbmMqSubscriptionProcessor(MessagingServiceDelegateService messagingServiceDelegateService) {
        super();
        this.messagingServiceDelegateService = messagingServiceDelegateService;
    }

    @Override
    public List<IbmMqSubscriptionEvent> handleEvent(Map<String, Object> properties, Void body) throws Exception {
        String messagingServiceId = (String) properties.get(RouteConstants.MESSAGING_SERVICE_ID);

        IbmMqHttpClient client = messagingServiceDelegateService.getMessagingServiceClient(messagingServiceId);

        log.info("### Invoking Queue endpoint for broker with ID {}", messagingServiceId);
        IbmMqSubscriptionResponse subscriptionList = client.getSubscriptions();

        return subscriptionList.getSubscription();
    }

}