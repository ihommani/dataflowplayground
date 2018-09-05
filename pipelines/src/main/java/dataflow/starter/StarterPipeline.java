package dataflow.starter;

import dataflow.common.WriteToBigQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example listen to a PubSub subscription to
 * <li>
 * <ul>serialize the payload into a Google BigQuery table</ul>
 * <ul>Resend a dummy message to the topic of the associated listened subscription</ul>
 * <ul>If the dummy message is the only message received, send a message to another topic</ul>
 * </li>
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=DataflowRunner
 */
@Slf4j
public class StarterPipeline {

    public interface StarterPipelineOptions extends PubsubOptions {

        @Description("Duration in seconds of a pane in a Global window")
        @Default.Long(5)
        Long getWindowDuration();
        void setWindowDuration(Long value);

        @Description("BigQuery Dataset to write tables to. Must already exist.")
        @Default.String("messages_test")
        String getDataset();
        void setDataset(String value);

        @Description("BigQuery Dataset to write tables to.")
        @Default.String("messages_table_test")
        String getDataTable();
        void setDataTable(String value);

        @Description("GCP Project ID")
        @Default.String("dataflow-report-test")
        String getProjectId();
        void setProjectId(String value);

        //TODO: Add new options: {input_subscription_name, something else? }
    }

    public static class FormatAsPubSubMessage extends SimpleFunction<Long, PubsubMessage> {
        @Override
        public PubsubMessage apply(Long message) {
            log.info("Creating killing message");
            return new PubsubMessage(String.valueOf(message).getBytes(), new HashMap<>());
        }
    }

    public static class CreateDummyPubSubMessage extends SimpleFunction<Long, PubsubMessage> {
        @Override
        public PubsubMessage apply(Long message) {
            log.info("Creating dummy");
            return new PubsubMessage("dummy".getBytes(), new HashMap<>());
        }
    }

    /**
     * Create a map of information that describes how to write pipeline output to BigQuery.
     */
    protected static Map<String, WriteToBigQuery.FieldInfo<KV<String, Long>>>
    configureBigQueryWrite() {
        Map<String, WriteToBigQuery.FieldInfo<KV<String, Long>>> tableConfigure = new HashMap<>();
        tableConfigure.put(
                "message", new WriteToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getKey()));
        tableConfigure.put(
                "occurrence",
                new WriteToBigQuery.FieldInfo<>("NUMERIC", (c, w) -> c.element().getValue()));
        return tableConfigure;
    }


    public static class CountMessages extends PTransform<PCollection<String>,
            PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> pubSubMessages) {

            // Count the number of times each message from the pubSub occurs.
            PCollection<KV<String, Long>> messageCounts = pubSubMessages.apply(Count.perElement());

            return messageCounts;
        }
    }

    static void runStarterPipeline(StarterPipelineOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

        PCollection<PubsubMessage> pubsubMessagePCollection = p.apply("Read PubSub messages", PubsubIO.readMessages().fromSubscription("projects/" + options.getProjectId() + "/subscriptions/bar_test"))
                .apply("windowing pipeline with sessions window", Window.<PubsubMessage>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(options.getWindowDuration()))))
                        //.triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1))) //TODO: combine number of element and ellapsed time since the first pane element;
                        .discardingFiredPanes());

        PCollection<Long> numberOfMessage = pubsubMessagePCollection
                .apply("counting pub/sub message", Count.globally());

        PDone killingBranch = numberOfMessage
                .apply("Filtering when PCollection size equals to one", Filter.equal(1L))
                .apply("PubSub message creation", MapElements.via(new FormatAsPubSubMessage()))
                .apply("Publishing on PusbSub kill topic", PubsubIO.writeMessages().to("projects/" + options.getProjectId() + "/topics/export_test"));

        PDone hearthBeatBranch = numberOfMessage
                .apply("Creating Dummy Message", MapElements.via(new CreateDummyPubSubMessage()))
                .apply("Publishing dummy message to the initial topic", PubsubIO.writeMessages().to("projects/" + options.getProjectId() + "/topics/foo_test"));

        PDone serialisationBranch = pubsubMessagePCollection
                .apply("PubSub message payload extraction", ParDo.of(new DoFn<PubsubMessage, String>() {
                    @ProcessElement
                    public void processElement(@Element PubsubMessage pubsubMessage, OutputReceiver<String> receiver) {
                        String element = new String(pubsubMessage.getPayload());
                        receiver.output(element);
                    }
                }))
                .apply("Filtering out dummy messages", Filter.by(input -> !input.matches("dummy.*")))
                .apply("Counting messages", new CountMessages())
                .apply("Inserting data to BQ table",

                        new WriteToBigQuery<>(
                                options.getProjectId(),
                                options.getDataset(),
                                options.getDataTable(),
                                configureBigQueryWrite()));

        PipelineResult result = p.run();
        log.info("jobId {} running", ((DataflowPipelineJob) result).getJobId());
        result.waitUntilFinish();
    }

    public static void main(String[] args) throws IOException {

        // from here we get the information from the command line argument
        // By default we return PipelineOptions entity. But we can return our own entity derived from PipelineOtions interfaces --> 'as(...)' method
        // In particular we define the pipeline runner through the options. If none is set, we use the DirectRunner (used to run pipeline locally)
        StarterPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(StarterPipelineOptions.class);

        // CLOUD
        options.setProject(options.getProjectId());
        options.setRunner(DataflowRunner.class);
        options.setStreaming(true);
        // LOCAL
        //options.setPubsubRootUrl("http://localhost:8085");
        log.info("Starting dataflow pipeline...");
        runStarterPipeline(options);
    }
}
