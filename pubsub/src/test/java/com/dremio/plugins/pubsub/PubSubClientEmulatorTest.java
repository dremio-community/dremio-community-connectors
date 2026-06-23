package com.dremio.plugins.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests against the Pub/Sub emulator.
 *
 * Requires the emulator to be running:
 *   docker run -d -p 8085:8681 messagebird/gcloud-pubsub-emulator
 *
 * And the emulator pre-seeded with a topic + subscription:
 *   export PUBSUB_EMULATOR_HOST=localhost:8085
 *   gcloud pubsub topics create orders
 *   gcloud pubsub subscriptions create orders-dremio --topic=orders
 *   # publish some messages
 *   gcloud pubsub topics publish orders --message='{"order_id":1,"customer":"Alice","amount":99.99}'
 *
 * Run with: mvn test -Dgroups=emulator
 */
@Tag("emulator")
class PubSubClientEmulatorTest {

  private static final String EMULATOR_HOST = "localhost:8085";
  private static final String PROJECT_ID    = "test-project";
  private static final String SUBSCRIPTION  = "orders-dremio";

  private static PubSubClient client;

  @BeforeAll
  static void setup() throws Exception {
    client = new PubSubClient(PROJECT_ID, "", EMULATOR_HOST);
    client.connect();
  }

  @Test
  void listSubscriptions() throws Exception {
    List<String> subs = client.listSubscriptions();
    assertFalse(subs.isEmpty(), "Expected at least one subscription in the emulator");
    assertTrue(subs.contains(SUBSCRIPTION),
        "Expected subscription '" + SUBSCRIPTION + "' in: " + subs);
  }

  @Test
  void pullAndNack() throws Exception {
    List<PubSubClient.PubSubMessage> msgs = client.pull(SUBSCRIPTION, 10);
    // Subscription may be empty — no assertion on count
    if (!msgs.isEmpty()) {
      PubSubClient.PubSubMessage first = msgs.get(0);
      assertNotNull(first.ackId);
      assertNotNull(first.messageId);
      assertFalse(first.data.length == 0 || first.dataAsUtf8().isEmpty(),
          "Expected non-empty message data");

      // NACK all — verify no exception
      List<String> ackIds = new java.util.ArrayList<>();
      for (PubSubClient.PubSubMessage m : msgs) ackIds.add(m.ackId);
      assertDoesNotThrow(() -> client.nack(SUBSCRIPTION, ackIds));
    }
  }

  @Test
  void messageDataIsValidJson() throws Exception {
    List<PubSubClient.PubSubMessage> msgs = client.pull(SUBSCRIPTION, 5);
    if (msgs.isEmpty()) return; // subscription drained — skip

    ObjectMapper mapper = new ObjectMapper();
    for (PubSubClient.PubSubMessage msg : msgs) {
      assertDoesNotThrow(() -> mapper.readTree(msg.dataAsUtf8()),
          "Message data should be valid JSON: " + msg.dataAsUtf8());
    }

    List<String> ackIds = new java.util.ArrayList<>();
    for (PubSubClient.PubSubMessage m : msgs) ackIds.add(m.ackId);
    client.nack(SUBSCRIPTION, ackIds);
  }
}
