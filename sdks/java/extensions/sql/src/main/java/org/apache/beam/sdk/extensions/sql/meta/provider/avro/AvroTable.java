/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.meta.provider.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.AvroRowCountEstimator;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@code AvroTable} is a {@link org.apache.beam.sdk.extensions.sql.BeamSqlTable} that reads and
 * writes Avro files.
 */
@Experimental
class AvroTable extends BaseBeamTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroTable.class);

  private final org.apache.avro.Schema avroSchema;
  private final String filePattern;

  AvroTable(Schema schema, String filePattern) {
    super(schema);
    this.avroSchema = AvroUtils.toAvroSchema(schema);
    this.filePattern = filePattern;
  }


  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply("ReadAvroFiles", AvroIO.readGenericRecords(avroSchema).from(filePattern))
        .apply("AvroToRow", MapElements
            .into(TypeDescriptors.rows())
            .via(AvroUtils.getGenericRecordToRowFunction(schema)));
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    return input
        .apply("RowToAvro", MapElements
            .into(TypeDescriptor.of(GenericRecord.class))
            .via(AvroUtils.getRowToGenericRecordFunction(avroSchema)))
        .apply("WriteAvroFiles", AvroIO.writeGenericRecords(avroSchema).to(filePattern));
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    try {
      double rowCount = new AvroRowCountEstimator(avroSchema, filePattern)
          .estimateRowCount(options);
      return BeamTableStatistics.createBoundedTableStatistics(rowCount);
    } catch (IOException e) {
      LOGGER.warn("Could not get the row count for the text table " + filePattern, e);
    }
    return BeamTableStatistics.BOUNDED_UNKNOWN;
  }
}
