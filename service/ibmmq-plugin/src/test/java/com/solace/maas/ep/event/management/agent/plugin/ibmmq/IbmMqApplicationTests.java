package com.solace.maas.ep.event.management.agent.plugin.ibmmq;

import com.solace.maas.ep.event.management.agent.plugin.ibmmq.client.http.IbmMqHttpClient;
import com.solace.maas.ep.event.management.agent.plugin.ibmmq.client.http.IbmMqQueueResponse;
import com.solace.maas.ep.event.management.agent.plugin.ibmmq.client.http.IbmMqSubscriptionResponse;
import com.solace.maas.ep.event.management.agent.plugin.ibmmq.manager.client.IbmMqClientManagerImpl;
import com.solace.maas.ep.event.management.agent.plugin.messagingService.event.AuthenticationDetailsEvent;
import com.solace.maas.ep.event.management.agent.plugin.messagingService.event.ConnectionDetailsEvent;
import com.solace.maas.ep.event.management.agent.plugin.messagingService.event.CredentialDetailsEvent;
import com.solace.maas.ep.event.management.agent.plugin.messagingService.event.EventProperty;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = IbmMqTestConfig.class)
@SuppressWarnings("PMD")
class IbmMqApplicationTests {

    public static MockWebServer mockWebServer;

    private ConnectionDetailsEvent connectionDetailsEvent;

    @BeforeAll
    static void setup() throws Exception {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

    }

    @BeforeEach
    void init() throws Exception {
        String baseurl = String.format("http://localhost:%s", mockWebServer.getPort());

        connectionDetailsEvent = ConnectionDetailsEvent.builder()
                .url(baseurl)
                .messagingServiceId("1")
                .name("name")
                .authenticationDetails(List.of(AuthenticationDetailsEvent.builder()
                        .credentials(List.of(CredentialDetailsEvent.builder()
                                .properties(List.of(
                                        EventProperty.builder()
                                                .name("username")
                                                .value("ush")
                                                .build(),
                                        EventProperty.builder()
                                                .name("password")
                                                .value("password")
                                                .build()))
                                .build()))
                        .build()))
                .build();
    }

    @Test
    void testGetQueues() throws Exception {
        IbmMqClientManagerImpl client = new IbmMqClientManagerImpl();
        IbmMqHttpClient httpClient = client.getClient(connectionDetailsEvent);

        Path filePath = Paths.get("src/test/resources/queue_response.json");
        String queueJson = Files.readString(filePath);

        mockWebServer.enqueue(new MockResponse()
                .setBody(queueJson)
                .addHeader("Content-Type", "application/json")
        );

        IbmMqQueueResponse response = httpClient.getQueues();

        long expected_queue_count = 8;

        Assert.assertEquals("Unexpected number of queues returned.", expected_queue_count, response.getQueue().size());
        Assert.assertEquals("First element does not match expected queue name,",
                "SYSTEM.ADMIN.STATISTICS.QUEUE", response.getQueue().get(0).getName());
    }

    @Test
    void testGetSubscriptions() throws Exception {
        IbmMqClientManagerImpl client = new IbmMqClientManagerImpl();
        IbmMqHttpClient httpClient = client.getClient(connectionDetailsEvent);

        //load our test file
        Path filePath = Paths.get("src/test/resources/subscription_response.json");
        String subJson = Files.readString(filePath);

        //add it to our Mock Webserver.
        //The server will respond with the contents of the test file
        mockWebServer.enqueue(new MockResponse()
                .setBody(subJson)
                .addHeader("Content-Type", "application/json")
        );

        IbmMqSubscriptionResponse response = httpClient.getSubscriptions();

        //ensure we received correct data
        long expected_topic_count = 15;

        Assert.assertEquals("Unexpected number of queues returned.", expected_topic_count, response.getSubscription().size());
        Assert.assertEquals("First element does not match expected subscription name,",
                "$SYS/MQ/INFO/QMGR/QM1/Monitor/CPU/SystemSummary", response.getSubscription().get(0).getResolvedTopicString());
    }

    @AfterAll
    static void shutDown() throws Exception {
        mockWebServer.shutdown();
    }
}
