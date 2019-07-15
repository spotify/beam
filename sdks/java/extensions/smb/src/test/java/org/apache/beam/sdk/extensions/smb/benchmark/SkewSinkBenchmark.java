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

package org.apache.beam.sdk.extensions.smb.benchmark;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.sdk.extensions.smb.avro.AvroFileOperations;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SkewSinkBenchmark {

  /** SinkOptions. */
  public interface SinkOptions extends PipelineOptions {
    String getOutputDir();

    void setOutputDir(String value);

    int getNumBuckets();

    void setNumBuckets(int value);

    int getNumShards();

    void setNumShards(int value);
  }


  private static final double s = 1.2;
  private static final int numKeys = 1000000;
  private static final int maxRecordsPerKey = 10000000;
  private static final int maxRecordsPerWorker = 100000;

  private static final Logger LOG = LoggerFactory.getLogger(SkewSinkBenchmark.class);
  private static final double normalizationFactor = 1.0 / getFrequency(numKeys);

  private static double getFrequency(long key) {
    return (1 / Math.pow(key, s));
  }

  private static long getNumOccurrencesForKey(long key) {
    return (int) Math.floor(getFrequency(key) * normalizationFactor);
  }

  public static void main(String[] args) throws Exception {
    final SinkOptions sinkOptions =
        PipelineOptionsFactory.fromArgs(args).as(SinkOptions.class);

    final Pipeline pipeline = Pipeline.create(sinkOptions);

    final PCollection<AvroGeneratedUser> left =
        pipeline
            .apply(Read.from(CountingSource.upTo(numKeys)))
            .apply(FlatMapElements.into(TypeDescriptors.kvs(
                TypeDescriptors.longs(), TypeDescriptors.integers())).via(key -> {
              long frequency = Math.min(maxRecordsPerKey, getNumOccurrencesForKey(key));
              if (frequency < maxRecordsPerWorker) {
                return ImmutableList.of(KV.of(key, (int) frequency));
              } else {
                LOG.info(
                    String.format("(%d, %d): for 0 -> %d, emit KV(%d, %d)",
                        key, frequency,
                        (int) Math.ceil(frequency / maxRecordsPerWorker), key,
                        maxRecordsPerWorker));

                return IntStream
                    .range(0, (int) Math.ceil(frequency / maxRecordsPerWorker)).boxed()
                    .map(shard -> KV.of(key, maxRecordsPerWorker))
                    .collect(Collectors.toList());
              }
            })).apply(
                FlatMapElements
                    .into(TypeDescriptor.of(AvroGeneratedUser.class))
                    .via(kv -> {
                      LOG.info(
                          String.format("Outputting: %d, %d times", kv.getKey(),
                              kv.getValue()));

                      return IntStream.rangeClosed(0, kv.getValue())
                        .boxed()
                        .map(
                            j ->
                                AvroGeneratedUser.newBuilder()
                                    .setName(String.format("user-%08d", kv.getKey()))
                                    .setFavoriteNumber(ThreadLocalRandom.current().nextInt())
                                    .setFavoriteColor(String.format("color-%08d", 5))
                                    .build())
                        .collect(Collectors.toList());
                    }));

    final ResourceId out = FileSystems.matchNewResource(sinkOptions.getOutputDir(), true);
    final ResourceId temp = FileSystems.matchNewResource(pipeline.getOptions().getTempLocation(), true);

    final AvroBucketMetadata<CharSequence, AvroGeneratedUser> avroMetadata =
        new AvroBucketMetadata<>(
            sinkOptions.getNumBuckets(),
            sinkOptions.getNumShards(),
            CharSequence.class,
            BucketMetadata.HashType.MURMUR3_32,
            "name");

    final SMBFilenamePolicy avroPolicy = new SMBFilenamePolicy(out, ".avro");
    final AvroFileOperations<AvroGeneratedUser> avroOps =
        AvroFileOperations.of(AvroGeneratedUser.class);
    final SortedBucketSink<CharSequence, AvroGeneratedUser> avroSink =
        new SortedBucketSink<>(avroMetadata, avroPolicy, avroOps::createWriter, temp);

    left.apply(avroSink);
//
//
//    // Non-skewed dataset to join with
//    final PCollection<AvroGeneratedUser> right =
//        pipeline
//            .apply(Read.from(CountingSource.upTo(numKeys)))
//            .apply(
//                FlatMapElements.into(TypeDescriptor.of(AvroGeneratedUser.class))
//                    .via(
//                        i ->
//                            IntStream.rangeClosed(0, 10)
//                                .boxed()
//                                .map(
//                                    j ->
//                                        AvroGeneratedUser.newBuilder()
//                                            .setName(String.format("user-%08d", i))
//                                            .setFavoriteNumber(j)
//                                            .setFavoriteColor(String.format("color-%08d", j))
//                                            .build())
//                                .collect(Collectors.toList())));
//


    pipeline.run();
  }
}
