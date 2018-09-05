package pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;

public class GCloudPubSubService {

    public void createTopic(String projectId, String topicName) {

        try (TopicAdminClient topicClient = TopicAdminClient.create()) {
            ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
            topicClient.createTopic(projectTopicName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createPullSubscription(String topicName, String subscriptionName) {

        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()) {
            // create a pull subscription with default acknowledgement deadline (= 10 seconds)
            subscriptionAdminClient.createSubscription(
                    subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
            subscriptionAdminClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void publish(String projectId, String topicName, int nbMessages) throws Exception {

        Publisher publisher = null;
        try {
            ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
            publisher = Publisher.newBuilder(projectTopicName).build();

            for (int i = 0; i < nbMessages; i++) {
                String message = "message-" + i;
                // convert message to bytes
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                        .setData(data)
                        .build();
                // Schedule a message to be published. Messages are automatically batched.
                publisher.publish(pubsubMessage);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // When finished with the publisher, shutdown to free up resources.
            if (publisher != null)
                publisher.shutdown();
        }
    }

}
