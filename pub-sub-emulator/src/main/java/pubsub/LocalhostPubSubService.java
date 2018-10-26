package pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LocalhostPubSubService {

    private String host;

    public void createTopic(String projectId, String topicName) {
        ManagedChannel channel = null;
        TopicAdminClient topicClient = null;
        try {
            channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
            TransportChannelProvider channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
            // Set the channel and credentials provider when creating a `TopicAdminClient`.
            // Similarly for SubscriptionAdminClient
            topicClient =
                    TopicAdminClient.create(
                            TopicAdminSettings.newBuilder()
                                    .setTransportChannelProvider(channelProvider)
                                    .setCredentialsProvider(credentialsProvider)
                                    .build());

            ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
            topicClient.createTopic(projectTopicName);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (channel != null)
                channel.shutdown();
            if (topicClient != null)
                topicClient.shutdown();
        }
    }

    public void createPullSubscription(String topicName, String subscriptionName) {

        ManagedChannel channel = null;
        SubscriptionAdminClient subscriptionAdminClient = null;
        try {
            channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
            TransportChannelProvider channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            CredentialsProvider credentialsProvider = NoCredentialsProvider.create();
            // Set the channel and credentials provider when creating a `TopicAdminClient`.
            // Similarly for SubscriptionAdminClient
            subscriptionAdminClient =
                    SubscriptionAdminClient.create(
                            SubscriptionAdminSettings.newBuilder()
                                    .setTransportChannelProvider(channelProvider)
                                    .setCredentialsProvider(credentialsProvider)
                                    .build());

            // create a pull subscription with default acknowledgement deadline (= 10 seconds)
            subscriptionAdminClient.createSubscription(
                    subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
            subscriptionAdminClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (channel != null)
                channel.shutdown();
            if (subscriptionAdminClient != null)
                subscriptionAdminClient.shutdown();

        }
    }

    public void publish(String projectId, String topicName, int nbMessages) throws Exception {

        Publisher publisher = null;
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
            TransportChannelProvider channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
            publisher = Publisher.newBuilder(projectTopicName)
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .build();

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
            if (channel != null)
                channel.shutdown();
        }
    }


    public void publish(String projectId, String topicName, PubsubMessage pubsubMessage) throws Exception {

        Publisher publisher = null;
        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forTarget(host).usePlaintext().build();
            TransportChannelProvider channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topicName);
            publisher = Publisher.newBuilder(projectTopicName)
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(NoCredentialsProvider.create())
                    .build();

            // Schedule a message to be published. Messages are automatically batched.
            publisher.publish(pubsubMessage);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // When finished with the publisher, shutdown to free up resources.
            if (publisher != null)
                publisher.shutdown();
            if (channel != null)
                channel.shutdown();
        }
    }

}
