package com.google.cloud.dataflow.examples.escience.seaflow;

import com.google.cloud.dataflow.examples.escience.seaflow.SeaFlowIO;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.SourceTestUtils;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.RemoveDuplicates;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.dataflow.examples.escience.seaflow.SeaFlowIO.SeaFlowRecord;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

/**
 * Tests for {@link SeaFlowIO}.
 */
@RunWith(JUnit4.class)
public class SeaFlowIOTest {

  @Test
  public void testReadingFile() throws Exception {
    TestPipeline p = TestPipeline.create();
    String testFilename = "/Users/dhalperi/IdeaProjects/eScience/data/37.evt";
    long fileSize = IOChannelUtils.getFactory(testFilename).getSizeBytes(testFilename);
    long expectedNumRecords = fileSize / SeaFlowIO.RECORD_SIZE_BYTES;

    PCollection<SeaFlowRecord> records = p.apply("Read SeaFlow", SeaFlowIO.readFrom(testFilename));
    DataflowAssert.thatSingleton(records.apply(Count.globally()))
        .isEqualTo(expectedNumRecords);
    DataflowAssert.thatSingleton(
        records
            .apply(MapElements.via((SeaFlowRecord r) -> r.particle)
                .withOutputType(new TypeDescriptor<Integer>() {}))
            .apply(RemoveDuplicates.create())
            .apply("CountUniques", Count.globally()))
        .isEqualTo(expectedNumRecords);

    p.run();
  }

  @Test
  public void testSourceSplits() throws Exception {
    String testFilename = "/Users/dhalperi/IdeaProjects/eScience/data/37.evt";
    SeaFlowIO.SeaFlowSource source = SeaFlowIO.readFrom(testFilename).getSource();
    SourceTestUtils
        .assertSourcesEqualReferenceSource(source, source.splitIntoBundles(100L, null), null);
  }

  @Test
  public void testSourceDynamicWorkRebalancing() throws Exception {
    String testFilename = "/Users/dhalperi/IdeaProjects/eScience/data/37.first4records.evt";
    SeaFlowIO.SeaFlowSource source = SeaFlowIO.readFrom(testFilename).getSource();
    SourceTestUtils.assertSplitAtFractionExhaustive(source, null);
  }
}