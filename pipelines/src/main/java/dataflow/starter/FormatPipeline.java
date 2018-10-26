package dataflow.starter;

import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.drive.model.FileList;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

@Slf4j
public class FormatPipeline {

    private static final String PROJECT_ID = "dataflow-report-test";
    private static final String SUBSCRIPTION = "projects/" + PROJECT_ID + "/subscriptions/format";
    private static final String TOPIC_TO_BQ = "projects/" + PROJECT_ID + "/topics/to_bq";

    public interface PipelineOptions extends PubsubOptions {

    }

    public static <T> Collection<T> fromJSON(final byte[] json, final Class<T> tClass) {
        try (InputStream inputStream = new ByteArrayInputStream(json)) {
            return JacksonFactory.getDefaultInstance()
                    .createJsonParser(inputStream)
                    .parseArrayAndClose(ArrayList.class, tClass);
        } catch (Exception e) {
            log.error("Can not deserialize pubsub message", e);
            return null;
        }
    }

    private static class DriveFormatFn extends DoFn<PubsubMessage, PubsubMessage> {
        @ProcessElement
        public void processElement(@Element PubsubMessage input, OutputReceiver<PubsubMessage> receiver) {
            try {
                Collection<FileList> fileLists = fromJSON(input.getPayload(), FileList.class);

                if (null != fileLists) {
                    log.info(fileLists.toString());
                    fileLists.stream()
                            .filter(Objects::nonNull)
                            .flatMap(fileList -> fileList.getFiles().stream())
                            .map(file -> file.set("properties", null))
                            .map(file -> file.setAppProperties(null))
                            .map(file -> new PubsubMessage(file.toString().getBytes(), input.getAttributeMap()))
                            .forEach(pubsubMessage -> receiver.output(pubsubMessage));
                }
                log.info("Done");
            } catch (Exception e) {
                log.error("Can not deserialize pubsub message", e);
            }
        }
    }

    private static class DriveTransform extends PTransform<PCollection<PubsubMessage>,
            PCollection<PubsubMessage>> {
        @Override
        public PCollection<PubsubMessage> expand(PCollection<PubsubMessage> input) {
            return input.apply(ParDo.of(new DriveFormatFn()));
        }
    }

    static void runStarterPipeline(PipelineOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);
        p.apply("Read PubSub messages", PubsubIO.readMessages().fromSubscription(SUBSCRIPTION))
                .apply("windowing pipeline with sessions window", Window.<PubsubMessage>into(new GlobalWindows()))
                //.apply("Filtering out drive message", Filter.by(pubsubMessage -> pubsubMessage.getAttribute("type").equals("drive")))
                .apply("Transform drive", new DriveTransform())
                .apply("Write PubSub messages", PubsubIO.writeMessages().to(TOPIC_TO_BQ));


        PipelineResult result = p.run();
        log.info("jobId {} running", ((DataflowPipelineJob) result).getJobId());
        result.waitUntilFinish();
    }

    public static void main(String[] args) throws IOException {

        // from here we get the information from the command line argument
        // By default we return PipelineOptions entity. But we can return our own entity derived from PipelineOtions interfaces --> 'as(...)' method
        // In particular we define the pipeline runner through the options. If none is set, we use the DirectRunner (used to run pipeline locally)
        FormatPipeline.PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(FormatPipeline.PipelineOptions.class);

        // CLOUD
        //options.setProject(options.getProjectId());
        //options.setRunner(DataflowRunner.class);
        //options.setStreaming(true);
        // LOCAL
        options.setPubsubRootUrl("http://localhost:8085");
        log.info("Starting dataflow pipeline...");
        runStarterPipeline(options);
    }

}
