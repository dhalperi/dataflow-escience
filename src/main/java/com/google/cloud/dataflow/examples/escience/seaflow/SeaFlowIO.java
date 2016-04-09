package com.google.cloud.dataflow.examples.escience.seaflow;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.FileBasedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.primitives.Ints;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.LittleEndianDataInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A transform to read SeaFlow files. For more information, see the <a
 * href="http://armbrustlab.ocean.washington.edu/resources/seaflow">SeaFlow website</a>.
 */
public final class SeaFlowIO {

  private static final Logger LOG = LoggerFactory.getLogger(SeaFlowIO.class);

  @DefaultCoder(AvroCoder.class)
  public static class SeaFlowRecord {
    // Required for Avro
    public SeaFlowRecord() {
      this("", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    public SeaFlowRecord(String filename, int particle, int time, int pulseWidth, int d1,
        int d2, int fscSmall, int fscPerp, int fscBig, int pe, int chlSmall, int chlBig) {
      this.filename = filename;
      this.particle = particle;
      this.time = time;
      this.pulseWidth = pulseWidth;
      this.d1 = d1;
      this.d2 = d2;
      this.fscSmall = fscSmall;
      this.fscPerp = fscPerp;
      this.fscBig = fscBig;
      this.pe = pe;
      this.chlSmall = chlSmall;
      this.chlBig = chlBig;
    }

    public final String filename;
    public final int particle;
    public final int time;
    public final int pulseWidth;
    public final int d1;
    public final int d2;
    public final int fscSmall;
    public final int fscPerp;
    public final int fscBig;
    public final int pe;
    public final int chlSmall;
    public final int chlBig;
  }

  public static class Read extends PTransform<PBegin, PCollection<SeaFlowRecord>> {
    private final String fileOrPatternSpec;
    private Read(String fileOrPatternSpec) {
      this.fileOrPatternSpec = checkNotNull(fileOrPatternSpec, "fileOrPatternSpec");
    }

    public PCollection<SeaFlowRecord> apply(PBegin input) {
      return input.apply(
          com.google.cloud.dataflow.sdk.io.Read.from(new SeaFlowSource(fileOrPatternSpec)));
    }

    @VisibleForTesting
    SeaFlowSource getSource() {
      return new SeaFlowSource(fileOrPatternSpec);
    }
  }

  public static Read readFrom(String fileOrPatternSpec) {
    return new Read(fileOrPatternSpec);
  }

  // A record is 10 shorts and then an int EOL
  @VisibleForTesting
  static final long RECORD_SIZE_BYTES = 10 * Short.BYTES + Integer.BYTES;
  // The SeaFlow EOL is an integer with specific value
  private static final int SEAFLOW_EOL = 10;

  @VisibleForTesting
  static class SeaFlowSource extends FileBasedSource<SeaFlowRecord> {

    /**
     * Constructs a new {@link SeaFlowSource} for the given file pattern.
     */
    public SeaFlowSource(String fileOrPatternSpec) {
      super(fileOrPatternSpec, RECORD_SIZE_BYTES);
    }

    /**
     * Constructs a new {@link SeaFlowSource} for the given subfile range.
     */
    public SeaFlowSource(String filename, long startOffset, long endOffset) {
      super(filename, RECORD_SIZE_BYTES, startOffset, endOffset);
    }

    @Override
    protected SeaFlowSource createForSubrangeOfFile(String filename, long start, long end) {
      return new SeaFlowSource(filename, start, end);
    }

    @Override
    protected FileBasedReader<SeaFlowRecord> createSingleFileReader(
        PipelineOptions pipelineOptions) {
      return new SeaFlowReader(this, pipelineOptions);
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions pipelineOptions) throws Exception {
      return false;
    }

    @Override
    public Coder<SeaFlowRecord> getDefaultOutputCoder() {
      return AvroCoder.of(SeaFlowRecord.class);
    }
  }

  private static final class SeaFlowReader extends FileBasedSource.FileBasedReader<SeaFlowRecord> {

    private transient LittleEndianDataInputStream input;
    private final PipelineOptions options;
    private final String filename;
    private long readOffset;
    @Nullable
    private SeaFlowRecord currentRecord;
    private long currentRecordStart;

    public SeaFlowReader(SeaFlowSource source, PipelineOptions options) {
      super(source);
      this.options = options;
      this.readOffset = source.getStartOffset();
      this.filename = source.getFileOrPatternSpec();
    }

    @Override
    public SeaFlowSource getCurrentSource() {
      return (SeaFlowSource) super.getCurrentSource();
    }

    private static long positiveMod(long number, long modulus) {
      long ret = number % modulus;
      if (ret < 0) {
        ret += modulus;
      }
      return ret;
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      input = new LittleEndianDataInputStream(new BufferedInputStream(Channels.newInputStream(channel)));
      // Seek to first record that begins after start offset.
      int seekAmount =
          Ints.checkedCast(positiveMod(Integer.BYTES - readOffset, RECORD_SIZE_BYTES));
      input.skipBytes(seekAmount);
      readOffset += seekAmount;
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      // Sanity check the reader state.
      checkState(readOffset % RECORD_SIZE_BYTES == Integer.BYTES,
          "Expected to be at the start of a record, but at position %s which is not"
              + " a multiple of record size %s", readOffset, RECORD_SIZE_BYTES);

      // Invalidate current record, prepare for next.
      currentRecord = null;
      // The record we might emit now starts here, and has a corresponding ID.
      currentRecordStart = readOffset;
      int particle = Ints.checkedCast((currentRecordStart - Integer.BYTES) / RECORD_SIZE_BYTES);

      // Every record except the last, including the header, is terminated byte the EOL.
      // To simplify implementation, we read the EOL before each line and use an EOF error
      // as an indicator that there are no more records.
      try {
        int eol = input.readInt();
        if (eol != SEAFLOW_EOL) {
          throw new IOException(
              String.format(
                  "Reading particle %d from file %s at byte offset %d, expected EOL but got %d",
                  particle,
                  filename,
                  readOffset,
                  eol));
        }
        readOffset += Integer.BYTES;
      } catch (EOFException e) {
        /* No more records. */
        return false;
      }

      // Read the next record.
      currentRecord = new SeaFlowRecord(filename,
          particle,
          input.readUnsignedShort(),
          input.readUnsignedShort(),
          input.readUnsignedShort(),
          input.readUnsignedShort(),
          input.readUnsignedShort(),
          input.readUnsignedShort(),
          input.readUnsignedShort(),
          input.readUnsignedShort(),
          input.readUnsignedShort(),
          input.readUnsignedShort());
      readOffset += 10 * Short.BYTES;

      return true;
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
      if (currentRecord == null) {
        throw new NoSuchElementException();
      }
      return currentRecordStart;
    }

    @Override
    public SeaFlowRecord getCurrent() throws NoSuchElementException {
      if (currentRecord == null) {
        throw new NoSuchElementException();
      }
      return currentRecord;
    }
  }
}
