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
package org.apache.beam.examples;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.TensorFlowFileOperations;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;

public class Features {
  public static void main(String[] args)
      throws Coder.NonDeterministicException, CannotProvideCoderException {
    System.out.println("hello");

    PCollection<Example> feature1 = null;
    PCollection<Example> feature2 = null;
    PCollection<Example> feature3 = null;
    PCollection<Example> feature4 = null;
    PCollection<Example> feature5 = null;
    PCollection<Example> feature6 = null;

    // save PCollection<Example> as SMB data, bucketed and sorted on composite key
    save(feature1, "gs://store/feature1", "user");
    save(feature2, "gs://store/feature2", "userId");
    save(feature3, "gs://store/feature3", "userGid");
    save(feature4, "gs://store/feature4", "user", "track");
    save(feature5, "gs://store/feature5", "userId", "trackId");
    save(feature6, "gs://store/feature6", "userGid", "trackGid");

    Pipeline pipeline = null;

    // read SMB data, with keys encoded in SMB metadata
    PCollection<KV<List<String>, Map<String, Iterable<Example>>>> userFeatures =
        readAndCoGroup(
            pipeline,
            ImmutableMap.of(
                "f1", "gs://store/feature1",
                "f2", "gs://store/feature2",
                "f3", "gs://store/feature3"));
    PCollection<KV<List<String>, Map<String, Iterable<Example>>>> userTrackFeatures =
        readAndCoGroup(
            pipeline,
            ImmutableMap.of(
                "f4", "gs://store/feature4",
                "f5", "gs://store/feature5",
                "f6", "gs://store/feature6"));
  }

  public static void save(PCollection<Example> data, String outputDirectory, String... keyFields)
      throws Coder.NonDeterministicException, CannotProvideCoderException {
    FeatureMetadata metadata = new FeatureMetadata(128, 8, Lists.newArrayList(keyFields));
    ResourceId output = FileSystems.matchNewResource(outputDirectory, true);
    TensorFlowFileOperations fileOperations = TensorFlowFileOperations.of(Compression.GZIP);
    SortedBucketSink<FeatureKey, Example> sink =
        new SortedBucketSink<>(metadata, output, ".tfrecord.gz", fileOperations);
    data.apply(sink);
  }

  // inputs is map of dataset id -> path
  // can replace output type `Map<String, Iterable<Example>>` with `Example` by merging sources.
  public static PCollection<KV<List<String>, Map<String, Iterable<Example>>>> readAndCoGroup(
      Pipeline pipeline, Map<String, String> inputs) {
    List<TupleTag<Example>> tupleTags = new ArrayList<>();
    List<BucketedInput<?, ?>> bucketedInputs = new ArrayList<>();
    for (Map.Entry<String, String> entry : inputs.entrySet()) {
      String id = entry.getKey();
      ResourceId inputDirectory = FileSystems.matchNewResource(entry.getValue(), true);
      TupleTag<Example> tupleTag = new TupleTag<>(id);
      BucketedInput<FeatureKey, Example> bucketedInput =
          new BucketedInput<>(
              tupleTag,
              inputDirectory,
              ".tfrecord.gz",
              TensorFlowFileOperations.of(Compression.GZIP));
      tupleTags.add(tupleTag);
      bucketedInputs.add(bucketedInput);
    }

    SortedBucketSource<FeatureKey> source =
        new SortedBucketSource<>(FeatureKey.class, bucketedInputs);
    return pipeline
        .apply(source)
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.lists(TypeDescriptors.strings()),
                        TypeDescriptors.maps(
                            TypeDescriptors.strings(),
                            TypeDescriptors.iterables(TypeDescriptor.of(Example.class)))))
                .via(
                    kv -> {
                      List<String> key = kv.getKey().get();
                      CoGbkResult result = kv.getValue();
                      Map<String, Iterable<Example>> values = new HashMap<>();

                      for (TupleTag<Example> tupleTag : tupleTags) {
                        values.put(tupleTag.getId(), result.getAll(tupleTag));
                      }
                      return KV.of(key, values);
                    }));
  }

  ////////////////////////////////////////////////////////////////////////////////

  // Existing TensorFlowBucketMetadata assumes a single key
  // This custom version supports composite keys
  public static class FeatureMetadata extends BucketMetadata<FeatureKey, Example> {

    @JsonProperty private final List<String> keyFields;

    public FeatureMetadata(int numBuckets, int numShards, List<String> keyFields)
        throws CannotProvideCoderException, Coder.NonDeterministicException {
      this(BucketMetadata.CURRENT_VERSION, numBuckets, numShards, keyFields);
    }

    @JsonCreator
    FeatureMetadata(
        @JsonProperty("version") int version,
        @JsonProperty("numBuckets") int numBuckets,
        @JsonProperty("numShards") int numShards,
        @JsonProperty("keyFields") List<String> keyFields)
        throws CannotProvideCoderException, Coder.NonDeterministicException {
      super(version, numBuckets, numShards, FeatureKey.class, HashType.MURMUR3_128);
      this.keyFields = keyFields;
    }

    @Override
    public FeatureKey extractKey(Example value) {
      List<String> keys =
          keyFields.stream()
              .map(
                  key -> {
                    Feature feature = value.getFeatures().getFeatureOrThrow(key);
                    Preconditions.checkState(feature.getKindCase() == Feature.KindCase.BYTES_LIST);
                    Preconditions.checkState(feature.getBytesList().getValueCount() == 1);
                    return feature.getBytesList().getValue(0).toStringUtf8();
                  })
              .collect(Collectors.toList());
      return new FeatureKey(keys);
    }

    @Override
    protected Map<Class<?>, Coder<?>> coderOverrides() {
      return ImmutableMap.of(FeatureKey.class, FeatureKeyCoder.of());
    }
  }

  ////////////////////////////////////////////////////////////////////////////////

  // Wrapper class to work around type erasure, e.g. impoosible to get a `Class<List<String>>`
  public static class FeatureKey {
    private final List<String> keys;

    public FeatureKey(List<String> keys) {
      this.keys = keys;
    }

    public List<String> get() {
      return keys;
    }
  }

  // Matching coder that delegates to Beam coders
  public static class FeatureKeyCoder extends AtomicCoder<FeatureKey> {
    private static final Coder<List<String>> CODER = ListCoder.of(StringUtf8Coder.of());
    private static final FeatureKeyCoder INSTANCE = new FeatureKeyCoder();

    private FeatureKeyCoder() {}

    public static FeatureKeyCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(FeatureKey value, OutputStream outStream)
        throws CoderException, IOException {
      CODER.encode(value.get(), outStream);
    }

    @Override
    public FeatureKey decode(InputStream inStream) throws CoderException, IOException {
      return new FeatureKey(CODER.decode(inStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      CODER.verifyDeterministic();
    }
  }
}
