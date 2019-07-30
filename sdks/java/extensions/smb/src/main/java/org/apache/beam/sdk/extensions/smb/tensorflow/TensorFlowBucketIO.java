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
package org.apache.beam.sdk.extensions.smb.tensorflow;

import org.apache.beam.sdk.extensions.smb.SortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.tensorflow.example.Example;

/** Abstracts SMB sources and sinks for TensorFlow Example records. */
public class TensorFlowBucketIO {
  private static final String DEFAULT_SUFFIX = ".tfrecord";

  public static <KeyT> SortedBucketSink<KeyT, Example> sink(
      TensorFlowMetadata<KeyT> bucketingMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      Compression compression) {
    return sink(bucketingMetadata, outputDirectory, tempDirectory, compression, null);
  }

  public static <KeyT> SortedBucketSink<KeyT, Example> sink(
      TensorFlowMetadata<KeyT> bucketingMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      Compression compression,
      String suffix) {
    Preconditions.checkArgument(
        compression != Compression.AUTO, "AUTO compression is not supported for writing");
    return SortedBucketIO.write(
        bucketingMetadata,
        outputDirectory,
        suffix != null ? suffix : DEFAULT_SUFFIX + compression.getSuggestedSuffix(),
        tempDirectory,
        TensorFlowFileOperations.of(compression));
  }

  public static <KeyT> BucketedInput<KeyT, Example> source(
      TupleTag<Example> tupleTag, ResourceId filenamePrefix, Compression compression) {
    return source(tupleTag, filenamePrefix, compression, null);
  }

  public static <KeyT> BucketedInput<KeyT, Example> source(
      TupleTag<Example> tupleTag,
      ResourceId filenamePrefix,
      Compression compression,
      String suffix) {
    return new BucketedInput<>(
        tupleTag,
        filenamePrefix,
        suffix != null ? suffix : DEFAULT_SUFFIX + compression.getSuggestedSuffix(),
        TensorFlowFileOperations.of(compression));
  }
}
