package pubsub;

public class PubSubStubRunner {
    //private static final String PROJECT_ID = "project-id";
    //private static final String HOST = "localhost:8085";
    //private static final LocalhostPubSubService pubSubService = new LocalhostPubSubService(HOST);
    private static final GCloudPubSubService pubSubService = new GCloudPubSubService();
    private static final String PROJECT_ID = "dataflow-report-test";

    private static final String TOPIC = "export_test";
    private static final String TOPIC_FOO = "foo_test";
    private static final String TOPIC_BQ = "bq_topic_test";
    private static final String TOPIC_SUBSCRIPTION = "projects/" + PROJECT_ID + "/topics/foo_test";
    private static final String SUBSCRIPTION = "projects/" + PROJECT_ID + "/subscriptions/bar_test";


    public static void main(String[] args) throws Exception {
        // For init (only once)
        //pubSubService.createTopic(PROJECT_ID, TOPIC);
        //pubSubService.createTopic(PROJECT_ID, TOPIC_FOO);
        //pubSubService.createTopic(PROJECT_ID, TOPIC_BQ);
        //pubSubService.createPullSubscription(TOPIC_SUBSCRIPTION, SUBSCRIPTION);

        // Send 5000 messages every 2 sec
        //for(int i=0; i<15; i++) {
            sendMessages(PROJECT_ID, TOPIC_FOO, 5000);
        //    Thread.sleep(2000);
        //}

    }

    public static void sendMessages(String projectId, String topic, int nbMessages) throws Exception {
        pubSubService.publish(projectId, topic, nbMessages);
    }

}
