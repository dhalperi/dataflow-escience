package com.google.cloud.dataflow.examples.escience.pentagon;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.examples.escience.pentagon.PdfIO.PdfPage;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableList;

/**
 * Created by dhalperi on 4/9/16.
 */
public class PdfDemo {

  private static class ExtractWordsFn extends DoFn<PdfPage, KV<PdfPage, String>> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      PdfPage page = c.element();
      String text = page.text;
      for (String word : text.split("[^'\\w]")) {
        if (word.isEmpty()) {
          continue;
        }
        c.output(KV.of(page, word));
      }
    }
  }

  private static final TableSchema TABLE_SCHEMA = new TableSchema().setFields(
      ImmutableList.of(
          new TableFieldSchema().setName("document").setType("STRING").setMode("REQUIRED"),
          new TableFieldSchema().setName("page").setType("INTEGER").setMode("REQUIRED"),
          new TableFieldSchema().setName("word").setType("STRING").setMode("REQUIRED")
      ));

  private static class TableRowFn extends DoFn<KV<PdfPage, String>, TableRow> {

    @Override
    public void processElement(ProcessContext c) throws Exception {
      PdfPage page = c.element().getKey();
      String word = c.element().getValue();
      TableRow row = new TableRow()
          .set("document", page.filename)
          .set("page", page.page)
          .set("word", word);
      c.output(row);
    }
  }

  interface Options extends PipelineOptions {

    @Validation.Required
    @Description("A pattern matching a collection of PDFs")
    String getInputFiles();
    void setInputFiles(String input);

    @Validation.Required
    @Description("A BigQuery table into which output can be written")
    String getOutputTable();
    void setOutputTable(String input);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
    Pipeline p = Pipeline.create(options);

    PCollection<PdfPage> records = p
        .apply("Extract pages from PDFs", PdfIO.Read.from(options.getInputFiles()));

    records
        .apply("Extract words from page", ParDo.of(new ExtractWordsFn()))
        .apply("Convert to TableRow", ParDo.of(new TableRowFn()))
        .apply("Write to BQ", BigQueryIO.Write.to(options.getOutputTable())
            .withSchema(TABLE_SCHEMA)
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));

    p.run();
  }
}