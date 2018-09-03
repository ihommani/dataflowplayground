package pubsub;

import java.io.IOException;

public class PubSubStubRunner {

    private static final LocalhostPubSubService pubSubService = new LocalhostPubSubService();
    private static final String HOST = "localhost:8085";
    private static final String PROJECT_ID = "project-id";
    private static final String topic = "my-topic";
    private static final String TOPIC_SINK = "sink";
    private static final String TOPIC_KILL = "kill";
    private static final String subscription = "projects/project-id/subscriptions/bar";


    public static void main(String[] args) throws Exception {
        /*
        pubSubService.createTopic(HOST, PROJECT_ID, topic);
        pubSubService.createTopic(HOST, PROJECT_ID, TOPIC_SINK);
        pubSubService.createTopic(HOST, PROJECT_ID, TOPIC_KILL);
        pubSubService.createPullSubscription(HOST, "projects/project-id/topics/my-topic", subscription);
        */


        sendMessages();
        //Thread.sleep(5000);
        //sendMessages();

    }

    public static void sendMessages() throws Exception {
        pubSubService.publish(HOST, PROJECT_ID, topic, 10000);
    }

}
