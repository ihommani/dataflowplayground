package com.ihommani.dataflow.starter;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
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


    public interface StarterPipelineOptions extends PipelineOptions {

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

    static class AddTimestampFn extends DoFn<String, String> {
        private final Instant minTimestamp;
        private final Instant maxTimestamp;

        AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
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
            extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

            return wordCounts;
        }
    }

    /**
     * A SimpleFunction that converts a Word and Count into a printable string.
     */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    static void runStarterPipeline(StarterPipelineOptions options) throws IOException {
        Pipeline p = Pipeline.create(options);

        final Instant minTimestamp = new Instant(options.getMinTimestampMillis());
        final Instant maxTimestamp = new Instant(options.getMaxTimestampMillis());

        PCollection<String> pipeline = p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply("dummy timestamp", ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));

        PCollection<String> windowedPipeline = pipeline
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(2L))));

        PCollection<KV<String, Long>> wordCount = windowedPipeline
                .apply("counting word", new CountWords());

        wordCount
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()).withSuffix(".txt"));

        PipelineResult result = p.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
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

        runStarterPipeline(options);
    }
}
