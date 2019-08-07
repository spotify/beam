package org.apache.beam.examples;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTableProvider;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.IOException;

public class BeamSqlExample {
  public static void main(String[] args) throws IOException {
    bigQueryTest();
  }

  static void bigQueryTest() throws IOException {
    String tableSpec = "publicdata:samples.shakespeare";
    String tableName = "shakespeare";
    TableReference tableRef = BigQueryHelpers.parseTableSpec(tableSpec);

    Bigquery bigquery = new Bigquery.Builder(
        new NetHttpTransport(),
        new JacksonFactory(),
        GoogleCredential.getApplicationDefault()
    ).build();

    TableSchema tableSchema = bigquery.tables()
        .get(tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId())
        .execute()
        .getSchema();
    Schema schema = BigQueryUtils.fromTableSchema(tableSchema);

    Table.Builder builder = Table.builder()
        .type("bigquery")
        .name(tableName)
        .schema(schema);

    BeamSqlTable inputTable = new BigQueryTableProvider()
        .buildBeamSqlTable(builder.location(tableSpec).build());

    String outputTableSpec = "scio-playground:neville_us.shakespeare_" + System.currentTimeMillis();
    BeamSqlTable outputTable = new BigQueryTableProvider()
        .buildBeamSqlTable(builder.location(outputTableSpec).build());

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setTempLocation("gs://scio-playground-us/neville");
    Pipeline pipeline = Pipeline.create(options);
    PCollection<Row> rows = inputTable.buildIOReader(pipeline.begin());

    rows
        .apply(Sample.any(10))
        .apply(MapElements.into(TypeDescriptors.rows()).via(row -> {
          System.out.println(row);
          return row;
        }));

    outputTable.buildIOWriter(rows);

    pipeline.run();
  }

  static BeamSqlTable avroTable(String tableName, Schema schema, String filepattern) {
    Table table = Table.builder()
        .name(tableName)
        .schema(schema)
        .location(filepattern)
        .build();
    // FIXME: need AvroTableProvider
    // https://issues.apache.org/jira/browse/BEAM-7920
    return null;
  }

  static BeamSqlTable bigQueryTable(String tableName, Schema schema, String tableSpec) {
    Table table = Table.builder()
        .name(tableName)
        .schema(schema)
        .location(tableSpec)
        .build();
    return new BigQueryTableProvider().buildBeamSqlTable(table);
  }
}
