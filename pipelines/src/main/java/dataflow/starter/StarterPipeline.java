package dataflow.starter;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.AfterAll;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
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


    /**
     * A {@link DefaultValueFactory} that returns the current system time.
     */
    public static class DefaultToCurrentSystemTime implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return System.currentTimeMillis();
        }
    }

    /**
     * A {@link DefaultValueFactory} that returns the minimum timestamp plus one hour.
     */
    public static class DefaultToMinTimestampPlusOneHour implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return options.as(StarterPipelineOptions.class).getMinTimestampMillis()
                    + Duration.standardHours(1).getMillis();
        }
    }


    public interface StarterPipelineOptions extends PubsubOptions {

        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();

        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Default.String("./output/")
        @Validation.Required
        String getOutput();

        void setOutput(String value);

        @Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToCurrentSystemTime.class)
        Long getMinTimestampMillis();

        void setMinTimestampMillis(Long value);

        @Description("Maximum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToMinTimestampPlusOneHour.class)
        Long getMaxTimestampMillis();

        void setMaxTimestampMillis(Long value);

        @Description("Duration in minutes of a fixe sized window")
        @Default.Long(5)
        Long getWindowDuration();

        void setWindowDuration(Long value);
    }

    /**
     * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
     * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to
     * a ParDo in the pipeline.
     */
    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = element.split("[^\\p{L}]+", -1);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    static class ExtractWordsMessage extends DoFn<PubsubMessage, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element PubsubMessage pubsubMessage, OutputReceiver<String> receiver, ProcessContext c) {
            String element = new String(pubsubMessage.getPayload());

            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }
            System.out.println(new String(pubsubMessage.getPayload()) + " " + c.timestamp());
            // Split the line into words.
            String[] words = element.split("[^\\p{L}]+", -1);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    static class AddTimestampFn extends DoFn<PubsubMessage, PubsubMessage> {
        private final Instant minTimestamp;
        private final Instant maxTimestamp;

        AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }

        @ProcessElement
        public void processElement(@Element PubsubMessage element, OutputReceiver<PubsubMessage> receiver) {
            Instant randomTimestamp =
                    new Instant(
                            ThreadLocalRandom.current()
                                    .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));

            /*
             * Concept #2: Set the data element with that timestamp.
             */
            receiver.outputWithTimestamp(element, new Instant(randomTimestamp));
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     *
     * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
     * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
     * modular testing, and an improved monitoring experience.
     */
    public static class CountWords
            extends PTransform<PCollection<PubsubMessage>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<PubsubMessage> messages) {
            PCollection<String> words = messages.apply(ParDo.of(new ExtractWordsMessage()));
            System.out.println("@@@@@@");
            // Count the number of times each word occurs.
            return words.apply(Count.perElement());
        }
    }

    /**
     * A SimpleFunction that converts a Word and Count into a printable string.
     */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            System.out.println(input.getKey() + ": " + input.getValue() + " " + new Date().toString());
            return input.getKey() + ": " + input.getValue();
        }
    }

    public static class FormatAsPubSubMessage extends SimpleFunction<String, PubsubMessage> {
        @Override
        public PubsubMessage apply(String message) {
            return new PubsubMessage("@@@@@".getBytes(), null);
        }
    }

    public static class SuicidePubSubMessage extends SimpleFunction<PubsubMessage, PubsubMessage> {
        @Override
        public PubsubMessage apply(PubsubMessage message) {
            return new PubsubMessage("killing me sofly".getBytes(), null);
        }
    }

    static void runStarterPipeline(StarterPipelineOptions options) throws IOException {


        final Instant minTimestamp = new Instant(options.getMinTimestampMillis());
        final Instant maxTimestamp = new Instant(options.getMaxTimestampMillis());
        Pipeline p = Pipeline.create(options);
        p.apply("ReadPubSubMessage", PubsubIO.readMessages().fromSubscription("projects/project-id/subscriptions/bar"))
                .apply("ApplyTimestamps", WithTimestamps.of((PubsubMessage pubSub) -> new Instant(System.currentTimeMillis())))
                .apply("SessionWindowing", Window.<PubsubMessage>into(Sessions.withGapDuration(Duration.standardSeconds(10)))
                        .triggering(DefaultTrigger.of())
                        .withAllowedLateness(Duration.standardSeconds(3))
                        .accumulatingFiredPanes())

                /*
                .apply("SessionWindowing", Window.<PubsubMessage>into(Sessions.withGapDuration(Duration.standardMinutes(1)))
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(3))).orFinally(AfterWatermark.pastEndOfWindow()))
                .withAllowedLateness(Duration.standardSeconds(3), Window.ClosingBehavior.FIRE_ALWAYS)
                .accumulatingFiredPanes())
                */
                .apply(new CountWords())
                //.apply("Combine", Combine.<String, Long>perKey(input -> ))
                //.apply(ParDo.named("FilterComplete").of(Foo.FilterComplete))
                .apply(MapElements.via(new FormatAsTextFn()));
        // Bug Apache Beam
        // https://stackoverflow.com/questions/46983318/writing-via-textio-write-with-sessions-windowing-raises-groupbykey-consumption-e
        //.apply("WriteCounts", TextIO.write().to(options.getOutput()).withWindowedWrites().withNumShards(1));

        //.apply("Apply timestamp", ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)))


        /*
        PCollection<PubsubMessage> pipeline = p.apply("ReadLines", PubsubIO.readMessages().fromSubscription("projects/project-id/subscriptions/bar"))

                .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));
        //TextIO assigns the same to each element. Using this PTransfom allow to mock event time. TODO: try WithTimestamps


        PCollection<PubsubMessage> windowedPipeline = pipeline
                .apply("windowing pipeline with sessions window", Window.<PubsubMessage>into(Sessions.withGapDuration(Duration.standardSeconds(10)))
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(3))).orFinally(AfterWatermark.pastEndOfWindow()))
                        .withAllowedLateness(Duration.standardSeconds(3))
                        .discardingFiredPanes());

        pipeline
                .apply("windowing pipeline with session window2", Window.<PubsubMessage>into(Sessions.withGapDuration(Duration.standardSeconds(10)))
                        .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(3))).orFinally(AfterWatermark.pastEndOfWindow()))
                        .withAllowedLateness(Duration.standardSeconds(3))
                        .discardingFiredPanes())
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply(MapElements.via(new FormatAsPubSubMessage()))
                .apply(MapElements.via(new SuicidePubSubMessage()))
                .apply(PubsubIO.writeMessages().to("projects/project-id/topics/kill"));


        PCollection<KV<String, Long>> wordCount = windowedPipeline
                .apply("counting word", new CountWords());

        wordCount
                .apply("Stringify key value pair", MapElements.via(new FormatAsTextFn()))
                .apply("creating pubsub message", MapElements.via(new FormatAsPubSubMessage()))
                .apply("writing one file per window", PubsubIO.writeMessages().to("projects/project-id/topics/sink"));
        */

        PipelineResult result = null;
        try {
            result = p.run();
            result.waitUntilFinish();
        } catch (Exception e) {
            e.printStackTrace();
            if (null != result)
                result.cancel();
        }

    }

    public static void main(String[] args) throws IOException {

        // from here we get the information from the command line argument
        // By default we return PipelineOptions entity. But we can return our own entity derived from PipelineOtions interfaces --> 'as(...)' method
        // In particular we define the pipeline runner through the options. If none is set, we use the DirectRunner (used to run pipeline locally)
        StarterPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(StarterPipelineOptions.class);

        options.setStreaming(true);
        options.setPubsubRootUrl("http://localhost:8085");

        runStarterPipeline(options);
    }
}
