package com.google.cloud.dataflow.examples.escience.pentagon;

import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.io.FileBasedSource;
import com.google.cloud.dataflow.sdk.io.FileBasedSource.FileBasedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;

import org.apache.pdfbox.cos.COSObjectKey;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.imageio.ImageIO;

/**
 * Created by dhalperi on 4/10/16.
 */
public class PdfIO {
  private static final Logger LOG = LoggerFactory.getLogger(PdfIO.class);

  public static class Read extends PTransform<PBegin, PCollection<KV<KV<String, Long>, byte[]>>> {
    private final String filePattern;

    private Read(String filePattern) {
      this.filePattern = filePattern;
    }

    public static Read from(String filePattern) {
      return new Read(filePattern);
    }

    @VisibleForTesting
    PdfSource getSource() {
      return new PdfSource(filePattern);
    }
  }

  @VisibleForTesting
  static class PdfSource extends FileBasedSource<KV<KV<String, Integer>, byte[]>> {
    private static final long BUNDLE_SIZE_BYTES = 100L*1024L; // 100KiB

    private PdfSource(String filePattern) {
      super(filePattern, BUNDLE_SIZE_BYTES);
    }

    private PdfSource(String filePattern, long start, long end) {
      super(filePattern, BUNDLE_SIZE_BYTES, start, end);
    }

    @Override
    protected PdfSource createForSubrangeOfFile(String fileName, long start, long end) {
      return new PdfSource(fileName, start, end);
    }

    @Override
    protected PdfReader createSingleFileReader(PipelineOptions options) {
      return new PdfReader(this);
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public Coder<KV<KV<String, Integer>, byte[]>> getDefaultOutputCoder() {
      return KvCoder.of(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()), ByteArrayCoder.of());
    }
  }

  private static class PdfReader extends FileBasedReader<KV<KV<String, Integer>, byte[]>> {
    private PDDocument document;
    private PDFRenderer renderer;
    private Map<COSObjectKey, Long> xrefTable;

    private int currentPage;
    private long currentPageOffset;
    private KV<KV<String, Integer>, byte[]> currentRecord;

    public PdfReader(PdfSource source) {
      super(source);
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      PdfSource source = getCurrentSource();
      if (source.getStartOffset() != 0) {
        SeekableByteChannel seekable = (SeekableByteChannel) channel;
        seekable.position(0L);
      }

      File tempFile = File.createTempFile("temp",".pdf");
      LOG.info("About to copy input file {} to local file {}", source.getFileOrPatternSpec(), tempFile.getCanonicalPath());
      FileOutputStream out = new FileOutputStream(tempFile);
      long copied = ByteStreams.copy(Channels.newInputStream(channel), out);
      out.close();
      LOG.info("Copied {} bytes from input file {} to local file {}", copied, source.getFileOrPatternSpec(), tempFile.getCanonicalPath());

      document = PDDocument.load(tempFile);
      renderer = new PDFRenderer(document);
      xrefTable = document.getDocument().getXrefTable();
      LOG.info("PDF Document {} has {} pages", source.getFileOrPatternSpec(), document.getNumberOfPages());

      currentPage = 0;
      currentPageOffset = -1;
      while (currentPage < document.getNumberOfPages() && currentPageOffset < source.getStartOffset()) {
        COSObjectKey pageKey = document.getDocument().getKey(document.getPage(currentPage).getCOSObject());
        currentPageOffset = xrefTable.get(pageKey);
        if (currentPageOffset >= source.getStartOffset()) {
          LOG.info("Starting at page {} [page offset {}, start offset {}]", currentPage, currentPageOffset, source.getStartOffset());
          break;
        }
        LOG.debug("Skipping page {} because its offset {} is less than the start offset {}", currentPage, currentPageOffset, source.getStartOffset());
        ++currentPage;
      }
    }

    @Override
    public synchronized PdfSource getCurrentSource() {
      return (PdfSource) super.getCurrentSource();
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      // Invalidate current record.
      currentRecord = null;
      if (currentPage >= document.getNumberOfPages()) {
        return false;
      }

      // Compute the current record.
      PDPage page = document.getPage(currentPage);
      COSObjectKey pageKey = document.getDocument().getKey(page.getCOSObject());
      currentRecord = KV.of(KV.of(getCurrentSource().getFileOrPatternSpec(), currentPage), pageToImage(currentPage));
      currentPageOffset = xrefTable.get(pageKey);

      // Advance to next page.
      ++currentPage;
      return true;
    }

    @Override
    public void close() throws IOException {
      super.close();
      if (document != null) {
        document.close();
        document = null;
      }
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
      if (currentRecord == null) {
        throw new NoSuchElementException();
      }
      return currentPageOffset;
    }

    @Override
    public KV<KV<String, Integer>, byte[]> getCurrent() throws NoSuchElementException {
      if (currentRecord == null) {
        throw new NoSuchElementException();
      }
      return currentRecord;
    }

    private byte[] pageToImage(int page) throws IOException {
      BufferedImage image = renderer.renderImageWithDPI(page, 300);
      try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
        ImageIO.write(image, "jpg", stream);
        stream.flush();
        return stream.toByteArray();
      }
    }
  }
}
