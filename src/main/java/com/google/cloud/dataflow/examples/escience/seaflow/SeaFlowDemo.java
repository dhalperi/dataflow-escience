package com.google.cloud.dataflow.examples.escience.seaflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.TextIO.Read;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.ApproximateQuantiles;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.collect.Ordering;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by dhalperi on 4/9/16.
 */
public class SeaFlowDemo {

  @DefaultCoder(AvroCoder.class)
  static class Record {

    public Record() {}

    public static Record of(String cruise, Instant time, int id, Particle p) {
      Record r = new Record();
      r.cruise = cruise;
      r.time = time;
      r.id = id;
      r.particle = p;
      return r;
    }

    String cruise;
    Instant time;
    int id;
    Particle particle;
  }

  @DefaultCoder(AvroCoder.class)
  static class Particle {

    public Particle() {}

    public static Particle of(int fscSmall, int fscPerp, int fscBig, int pe, int chlSmall,
        int chlBig) {
      Particle p = new Particle();
      p.fscSmall = fscSmall;
      p.fscPerp = fscPerp;
      p.fscBig = fscBig;
      p.pe = pe;
      p.chlSmall = chlSmall;
      p.chlBig = chlBig;
      return p;
    }

    int fscSmall;
    int fscPerp;
    int fscBig;
    int pe;
    int chlSmall;
    int chlBig;
  }

  private static class ParseCsv extends DoFn<String, Record> {

    private static final DateTimeFormatter FORMATTER = DateTimeFormat
        .forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();

    @Override
    public void processElement(ProcessContext c) throws Exception {
      String[] input = c.element().split(",");
      String cruise = input[0];
      Instant time = Instant.parse(input[1], FORMATTER);
      int id = Integer.valueOf(input[2]);
      // Skip time=3, pulseWidth=4, d1=5, d2=6
      int fscSmall = Integer.valueOf(input[7]);
      int fscPerp = Integer.valueOf(input[8]);
      int fscBig = Integer.valueOf(input[9]);
      int pe = Integer.valueOf(input[10]);
      int chlSmall = Integer.valueOf(input[11]);
      int chlBig = Integer.valueOf(input[12]);
      Particle p = Particle.of(fscSmall, fscPerp, fscBig, pe, chlSmall, chlBig);
      c.outputWithTimestamp(Record.of(cruise, time, id, p), time);
    }
  }

  interface Options extends PipelineOptions {

    @Validation.Required
    @Description("A CSV file containing SeaFlow data")
    String getInput();
    void setInput(String input);

    @Validation.Required
    @Description("A directory into which output can be written")
    String getOutputDirectory();
    void setOutputDirectory(String input);
  }

  private static class ComputeInterestingQuantile extends
      PTransform<PCollection<KV<String, Long>>, PCollectionView<Map<String, Long>>> {
    private final int count;
    public ComputeInterestingQuantile(int count) {
      this.count = count;
    }
    @Override
    public PCollectionView<Map<String, Long>> apply(PCollection<KV<String, Long>> windowedRecords) {
      return windowedRecords.apply("Global Approximate Quantiles", Window.into(new GlobalWindows()))
          .apply("Compute approximate quantiles", ApproximateQuantiles
              .perKey(count, (Comparator<Long> & Serializable) Ordering.<Long>natural()))
          .apply("Keep second-highest quantile",
              MapElements
                  .via((KV<String, List<Long>> k) -> KV.of(k.getKey(), k.getValue().get(count - 2)))
                  .withOutputType(new TypeDescriptor<KV<String, Long>>() {}))
          .apply("Construct quantile Map", View.asMap());
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
    Pipeline p = Pipeline.create(options);

    PCollection<KV<String, Record>> records = p
        .apply("Read CSV", Read.from(options.getInput()))
        .apply("Parse CSV to SeaFlow", ParDo.of(new ParseCsv()))
        .apply("Key by Cruise", MapElements.via((Record r) -> KV.of(r.cruise, r)).withOutputType(
            new TypeDescriptor<KV<String, Record>>() {}));

    records.apply("Count per cruise", Count.perKey())
        .apply("Stringify",
            MapElements.via((KV<String, Long> k) -> k.getKey() + ',' + k.getValue())
                .withOutputType(new TypeDescriptor<String>() {}))
        .apply("Write to count-cruise.txt",
            TextIO.Write.to(options.getOutputDirectory() + "/count-cruise.txt").withoutSharding());

    PCollection<KV<String, Record>> windowedRecords =
        records.apply("5min windows", Window.into(FixedWindows.of(Duration.standardMinutes(5))));

    PCollection<KV<String, Long>> windowedCounts =
        windowedRecords.apply("Windowed count", Count.perKey());

    PCollectionView<Map<String, Long>> percentileView =
        windowedCounts.apply("Compute 80th percentile", new ComputeInterestingQuantile(5));

    windowedCounts.apply("Filter interesting windows",
        ParDo.of(new DoFn<KV<String, Long>, KV<String, Long>>() {
          @Override
          public void processElement(ProcessContext c) throws Exception {
            KV<String, Long> windowedCount = c.element();
            if (windowedCount.getValue() >= c.sideInput(percentileView)
                .get(windowedCount.getKey())) {
              c.output(windowedCount);
            }
          }
        }).withSideInputs(percentileView))
        .apply("Interesting sessions", Window.<KV<String, Long>>into(Sessions.withGapDuration(Duration.standardMinutes(20))))
        .apply("Compute windows/session", Count.perKey())
        .apply("Stringify sessions", ParDo.of(new DoFnWithContext<KV<String, Long>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c, BoundedWindow window) {
            if (c.element().getValue() < 5) {
              // skip a session with fewer than 5 interesting windows.
              return;
            }
            String message =
                String.format("%s has %d interesting 5-minute windows in the time range %s",
                    c.element().getKey(), c.element().getValue(), window);
            c.output(message);
          }
        }))
        .apply("Global window", Window.into(new GlobalWindows()))
        .apply("Write cruise_interesting_windows",
            TextIO.Write.to(options.getOutputDirectory() + "/cruise_interesting_windows")
                .withSuffix(".txt"));

    p.run();
  }
}