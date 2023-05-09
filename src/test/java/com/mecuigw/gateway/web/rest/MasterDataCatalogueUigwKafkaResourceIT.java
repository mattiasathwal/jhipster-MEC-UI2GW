package com.mecuigw.gateway.web.rest;

import static org.assertj.core.api.Assertions.assertThat;

import com.mecuigw.gateway.IntegrationTest;
import com.mecuigw.gateway.config.EmbeddedKafka;
import com.mecuigw.gateway.config.KafkaSseConsumer;
import com.mecuigw.gateway.config.KafkaSseProducer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.util.MimeTypeUtils;

@IntegrationTest
@AutoConfigureMockMvc
@WithMockUser
@EmbeddedKafka
class MasterDataCatalogueUigwKafkaResourceIT {

    @Autowired
    private WebTestClient client;

    @Autowired
    @Qualifier(KafkaSseProducer.CHANNELNAME)
    private MessageChannel output;

    @Autowired
    @Qualifier(KafkaSseConsumer.CHANNELNAME)
    private MessageChannel input;

    @Autowired
    private MessageCollector collector;

    @Test
    void producesMessages() throws InterruptedException {
        client.post().uri("/api/master-data-catalogue-uigw-kafka/publish?message=value-produce").exchange().expectStatus().isNoContent();

        BlockingQueue<Message<?>> messages = collector.forChannel(output);
        GenericMessage<String> payload = (GenericMessage<String>) messages.take();
        assertThat(payload.getPayload()).isEqualTo("value-produce");
    }

    @Test
    void consumesMessages() {
        Map<String, Object> map = new HashMap<>();
        map.put(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);
        MessageHeaders headers = new MessageHeaders(map);
        Message<String> testMessage = new GenericMessage<>("value-consume", headers);
        input.send(testMessage);
        String value = client
            .get()
            .uri("/api/master-data-catalogue-uigw-kafka/consume")
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus()
            .isOk()
            .expectHeader()
            .contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
            .returnResult(String.class)
            .getResponseBody()
            .blockFirst(Duration.ofSeconds(10));
        assertThat(value).isEqualTo("value-consume");
    }
}
