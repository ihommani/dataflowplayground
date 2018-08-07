package com.ihommani.dataflow.starter;

import com.ihommani.dataflow.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class WindowedWordCount {
    static final int WINDOW_SIZE = 10; // Default window duration in minutes

    /**
     * Concept #2: A DoFn that sets the data element timestamp. This is a silly method, just for this
     * example, for the bounded data case.
     *
     * <p>Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
     * his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
     * 2-hour period.
     */
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
            return options.as(Options.class).getMinTimestampMillis()
                    + Duration.standardHours(1).getMillis();
        }
    }

    /**
     * Options for {@link WindowedWordCount}.
     *
     * <p>Inherits standard example configuration options, which allow specification of the runner, as
     * well as the {@link WordCount.WordCountOptions} support for specification of the input and
     * output files.
     */
    public interface Options
            extends WordCount.WordCountOptions {
        @Description("Fixed window duration, in minutes")
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        @Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToCurrentSystemTime.class)
        Long getMinTimestampMillis();

        void setMinTimestampMillis(Long value);

        @Description("Maximum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToMinTimestampPlusOneHour.class)
        Long getMaxTimestampMillis();

        void setMaxTimestampMillis(Long value);

        @Description("Fixed number of shards to produce per window")
        Integer getNumShards();

        void setNumShards(Integer numShards);
    }

    static void runWindowedWordCount(Options options) throws IOException {
        final String output = options.getOutput();
        final Instant minTimestamp = new Instant(options.getMinTimestampMillis());
        final Instant maxTimestamp = new Instant(options.getMaxTimestampMillis());

        Pipeline pipeline = Pipeline.create(options);

        /*
         * Concept #1: the Beam SDK lets us run the same pipeline with either a bounded or
         * unbounded input source.
         */
        PCollection<String> input =
                pipeline
                        /* Read from the GCS file. */
                        .apply(TextIO.read().from(options.getInputFile()))
                        // Concept #2: Add an element timestamp, using an artificial time just to show windowing.
                        // See AddTimestampFn for more detail on this.
                        .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));

        /*
         * Concept #3: Window into fixed windows. The fixed window size for this example defaults to 1
         * minute (you can change this with a command-line option). See the documentation for more
         * information on how fixed windows work, and for information on the other types of windowing
         * available (e.g., sliding windows).
         */
        PCollection<String> windowedWords =
                input.apply(
                        Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));

        /*
         * Concept #4: Re-use our existing CountWords transform that does not have knowledge of
         * windows over a PCollection containing windowed values.
         */
        PCollection<KV<String, Long>> wordCounts = windowedWords.apply(new WordCount.CountWords());

        /*
         * Concept #5: Format the results and write to a sharded file partitioned by window, using a
         * simple ParDo operation. Because there may be failures followed by retries, the
         * writes must be idempotent, but the details of writing to files is elided here.
         */
        wordCounts
                .apply(MapElements.via(new WordCount.FormatAsTextFn()))
                .apply(new WriteOneFilePerWindow(output, options.getNumShards()));

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    public static void main(String[] args) throws IOException {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        runWindowedWordCount(options);
    }
}
