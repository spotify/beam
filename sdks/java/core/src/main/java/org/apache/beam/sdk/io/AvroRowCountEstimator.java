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
package org.apache.beam.sdk.io;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;

/** This returns a row count estimation for files associated with a file pattern. */
public class AvroRowCountEstimator {
  private static final long SAMPLE_BYTES_PER_FILE = 64 * 1024L;
  private static final long TOTAL_SAMPLE_BYTES = 1024 * 1024L;

  private final Schema schema;
  private final String filePattern;

  public AvroRowCountEstimator(Schema schema, String filePattern) {
    this.schema = schema;
    this.filePattern = filePattern;
  }

  /**
   * Estimate the number of Avro records. It samples SAMPLE_BYTES_PER_FILE bytes from every file
   * until TOTAL_SAMPLE_BYTES are sampled. Then it takes the average record size of the rows and
   * divides the total file sizes by that number.
   */
  public double estimateRowCount(PipelineOptions pipelineOptions) throws IOException {
    long totalFileBytes = 0;
    long sampledBytes = 0;
    int sampledRecords = 0;

    MatchResult match = FileSystems.match(filePattern, EmptyMatchTreatment.DISALLOW);

    for (MatchResult.Metadata metadata : match.metadata()) {
      if (FileIO.ReadMatches.shouldSkipDirectory(
          metadata, FileIO.ReadMatches.DirectoryTreatment.SKIP)) {
        continue;
      }

      long fileSize = metadata.sizeBytes();
      totalFileBytes += fileSize;

      if (totalFileBytes > TOTAL_SAMPLE_BYTES) {
        continue;
      }

      FileIO.ReadableFile file =
          FileIO.ReadMatches.matchToReadableFile(metadata, Compression.UNCOMPRESSED);

      long sampleSize = Math.min(SAMPLE_BYTES_PER_FILE, fileSize);
      try (BoundedSource.BoundedReader<GenericRecord> reader =
          AvroSource.from(file.getMetadata().resourceId().toString())
              .withSchema(schema)
              .createForSubrangeOfFile(metadata, 0, sampleSize)
              .createReader(pipelineOptions)) {
        for (boolean more = reader.start(); more; more = reader.advance()) {
          sampledRecords++;
        }
        sampledBytes += sampleSize;
      }
    }
    // This is total file sizes divided by average record size.
    return (double) totalFileBytes / sampledBytes * sampledRecords;
  }
}
