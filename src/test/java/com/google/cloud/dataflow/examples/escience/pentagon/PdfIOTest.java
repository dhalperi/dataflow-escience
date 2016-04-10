package com.google.cloud.dataflow.examples.escience.pentagon;

import static org.junit.Assert.*;

import com.google.cloud.dataflow.sdk.testing.SourceTestUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Created by dhalperi on 4/10/16.
 */
@RunWith(JUnit4.class)
public class PdfIOTest {
  @Test
  public void testPdfIORead() throws Exception {
    PdfIO.PdfSource source = PdfIO.Read.from("/Users/dhalperi/Downloads/Pentagon-Papers-Part-I.pdf").getSource();
    SourceTestUtils.readFromSource(source, null);
  }

  @Test
  public void testSourceSplits() throws Exception {
    PdfIO.PdfSource source = PdfIO.Read.from("/Users/dhalperi/Downloads/Pentagon-Papers-Index.pdf").getSource();
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.splitIntoBundles(1024*1024, null), null);
  }
}