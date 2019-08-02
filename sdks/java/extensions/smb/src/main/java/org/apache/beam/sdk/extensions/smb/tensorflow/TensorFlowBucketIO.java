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

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.tensorflow.example.Example;

/** Abstracts SMB sources and sinks for TensorFlow Example records. */
public class TensorFlowBucketIO {
  private static final String DEFAULT_SUFFIX = ".tfrecord";

  public static Read read(TupleTag<Example> tupleTag) {
    return new AutoValue_TensorFlowBucketIO_Read.Builder()
        .setTupleTag(tupleTag)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCompression(Compression.AUTO)
        .build();
  }

  public static <K> Write<K> write(Class<K> keyClass, String keyField) {
    return new AutoValue_TensorFlowBucketIO_Write.Builder<K>()
        .setNumBuckets(SortedBucketIO.DEFAULT_NUM_BUCKETS)
        .setNumShards(SortedBucketIO.DEFAULT_NUM_SHARDS)
        .setHashType(SortedBucketIO.DEFAULT_HASH_TYPE)
        .setSorterMemoryMb(SortedBucketIO.DEFAULT_SORTER_MEMORY_MB)
        .setKeyClass(keyClass)
        .setKeyField(keyField)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCompression(Compression.UNCOMPRESSED)
        .build();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Read
  ////////////////////////////////////////////////////////////////////////////////

  @AutoValue
  public abstract static class Read extends SortedBucketIO.Read<Example> {
    abstract TupleTag<Example> getTupleTag();

    @Nullable
    abstract ResourceId getFilenamePrefix();

    abstract String getFilenameSuffix();

    abstract Compression getCompression();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTupleTag(TupleTag<Example> tupleTag);

      abstract Builder setFilenamePrefix(ResourceId filenamePrefix);

      abstract Builder setFilenameSuffix(String filenameSuffix);

      abstract Builder setCompression(Compression compression);

      abstract Read build();
    }

    public Read from(String filenamePrefix) {
      return toBuilder()
          .setFilenamePrefix(FileSystems.matchNewResource(filenamePrefix, true))
          .build();
    }

    public Read withFilenameSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    @Override
    protected BucketedInput<?, Example> toBucketedInput() {
      return new BucketedInput<>(
          getTupleTag(),
          getFilenamePrefix(),
          getFilenameSuffix(),
          TensorFlowFileOperations.of(getCompression()));
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Write
  ////////////////////////////////////////////////////////////////////////////////

  /** Write records to sorted bucket files. */
  @AutoValue
  public abstract static class Write<K> extends PTransform<PCollection<Example>, WriteResult> {
    // Common
    @Nullable
    abstract BucketMetadata<K, Example> getMetadata();

    abstract int getNumBuckets();

    abstract int getNumShards();

    abstract Class<K> getKeyClass();

    abstract HashType getHashType();

    @Nullable
    abstract ResourceId getOutputDirectory();

    @Nullable
    abstract ResourceId getTempDirectory();

    abstract String getFilenameSuffix();

    abstract int getSorterMemoryMb();

    // TFRecord specific
    @Nullable
    abstract String getKeyField();

    abstract Compression getCompression();

    abstract Builder<K> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K> {
      // Common
      abstract Builder<K> setMetadata(BucketMetadata<K, Example> metadata);

      abstract Builder<K> setNumBuckets(int numBuckets);

      abstract Builder<K> setNumShards(int numShards);

      abstract Builder<K> setKeyClass(Class<K> keyClass);

      abstract Builder<K> setHashType(HashType hashType);

      abstract Builder<K> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K> setSorterMemoryMb(int sorterMemoryMb);

      // TFRecord specific
      abstract Builder<K> setKeyField(String keyField);

      abstract Builder<K> setCompression(Compression compression);

      abstract Write<K> build();
    }

    public Write<K> withMetadata(BucketMetadata<K, Example> metadata) {
      return toBuilder().setMetadata(metadata).build();
    }

    public Write<K> withNumBuckets(int numBuckets) {
      return toBuilder().setNumBuckets(numBuckets).build();
    }

    public Write<K> withNumShards(int numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    public Write<K> withHashType(HashType hashType) {
      return toBuilder().setHashType(hashType).build();
    }

    public Write<K> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    public Write<K> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    public Write<K> withFilenameSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    public Write<K> withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    @Override
    public WriteResult expand(PCollection<Example> input) {
      Preconditions.checkNotNull(getOutputDirectory(), "outputDirectory is not set");

      BucketMetadata<K, Example> metadata = getMetadata();
      if (metadata == null) {
        try {
          metadata =
              new TensorFlowMetadata<>(
                  getNumBuckets(), getNumShards(), getKeyClass(), getHashType(), getKeyField());
        } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
          throw new IllegalStateException(e);
        }
      }

      final ResourceId outputDirectory = getOutputDirectory();
      ResourceId tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = outputDirectory;
      }

      final TensorFlowFileOperations fileOperations = TensorFlowFileOperations.of(getCompression());
      SortedBucketSink<K, Example> sink =
          new SortedBucketSink<>(
              metadata,
              outputDirectory,
              tempDirectory,
              getFilenameSuffix(),
              fileOperations,
              getSorterMemoryMb());
      return input.apply(sink);
    }
  }
}
