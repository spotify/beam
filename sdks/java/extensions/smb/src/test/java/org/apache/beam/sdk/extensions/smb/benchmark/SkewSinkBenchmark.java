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
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/** Sink benchmark for skewed data following a zipf distribution. */
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

  // @TODO: Tune these values until job starts to fail with a single shard
  private static final double s = 1.8;
  private static final int numKeys = 500000;

  // For key 0, frequency can be > 2 billion which isn't realistic either
  private static final int maxRecordsPerKey = 10000000;
  private static final int maxRecordsPerWorker = 50000;

  // Multiplication factor so there is a minimum of 1 occurrence per key
  private static final double normalizationFactor = 1.0 / getFrequency(numKeys);

  // Use a zipf distribution with provided value of `s`
  private static double getFrequency(long key) {
    return (1 / Math.pow(key + 0.001, s));
  }

  private static long getNumOccurrencesForKey(long key) {
    return (int) Math.floor(getFrequency(key) * normalizationFactor);
  }

  public static void main(String[] args) throws Exception {
    final SinkOptions sinkOptions = PipelineOptionsFactory.fromArgs(args).as(SinkOptions.class);

    final Pipeline pipeline = Pipeline.create(sinkOptions);

    final PCollection<AvroGeneratedUser> skewData =
        pipeline
            .apply(GenerateSequence.from(0).to(numKeys))
            .apply(
                FlatMapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.integers()))
                    .via(
                        key -> {
                          long frequency = Math.min(maxRecordsPerKey, getNumOccurrencesForKey(key));

                          if (frequency < maxRecordsPerWorker) {
                            return ImmutableList.of(KV.of(key, (int) frequency));
                          } else {
                            // If there's too many occurrences to fit in one in-memory list shard
                            // it out across workers
                            return IntStream.range(0, (int) frequency / maxRecordsPerWorker)
                                .boxed()
                                .map(shard -> KV.of(key, maxRecordsPerWorker))
                                .collect(Collectors.toList());
                          }
                        }))
            .apply(
                FlatMapElements.into(TypeDescriptor.of(AvroGeneratedUser.class))
                    .via(
                        kv ->
                            IntStream.rangeClosed(0, kv.getValue())
                                .boxed()
                                .map(
                                    j ->
                                        AvroGeneratedUser.newBuilder()
                                            .setName(String.format("user-%08d", kv.getKey()))
                                            .setFavoriteNumber(
                                                ThreadLocalRandom.current().nextInt())
                                            .setFavoriteColor(String.format("color-%08d", 5))
                                            .build())
                                .collect(Collectors.toList())));

    final ResourceId out = FileSystems.matchNewResource(sinkOptions.getOutputDir(), true);
    final ResourceId temp =
        FileSystems.matchNewResource(pipeline.getOptions().getTempLocation(), true);

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

    skewData.apply(avroSink);

    pipeline.run();
  }
}
