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
package org.apache.beam.sdk.extensions.smb.json;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** Abstracts SMB sources and sinks for JSON records. */
public class JsonSortedBucketIO {
  private static final String DEFAULT_SUFFIX = ".json";

  public static <KeyT> SortedBucketSink<KeyT, TableRow> sink(
      JsonBucketMetadata<KeyT> metadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      Compression compression) {
    Preconditions.checkArgument(
        compression != Compression.AUTO, "AUTO compression is not supported for writing");
    return SortedBucketIO.write(
        metadata,
        outputDirectory,
        tempDirectory,
        filenameSuffix != null ? filenameSuffix : DEFAULT_SUFFIX + compression.getSuggestedSuffix(),
        JsonFileOperations.of(compression));
  }

  public static <KeyT> BucketedInput<KeyT, TableRow> source(
      TupleTag<TableRow> tupleTag,
      ResourceId filenamePrefix,
      String filenameSuffix,
      Compression compression) {
    return new BucketedInput<>(
        tupleTag,
        filenamePrefix,
        filenameSuffix != null ? filenameSuffix : DEFAULT_SUFFIX + compression.getSuggestedSuffix(),
        JsonFileOperations.of(compression));
  }
}
