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
package org.apache.beam.sdk.extensions.smb.avro;

import static org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/** Abstracts SMB sources and sinks for Avro-typed values. */
public class AvroSortedBucketIO {
  private static final String DEFAULT_SUFFIX = ".avro";

  ////////////////////////////////////////////////////////////////////////////////
  // Write
  ////////////////////////////////////////////////////////////////////////////////

  public static <K> Write<K, GenericRecord> write(
      Class<K> keyClass, String keyField, Schema schema) {
    return AvroSortedBucketIO.newBuilder(keyClass, keyField).setSchema(schema).build();
  }

  public static <K, T extends SpecificRecordBase> Write<K, T> write(
      Class<K> keyClass, String keyField, Class<T> recordClass) {
    return AvroSortedBucketIO.<K, T>newBuilder(keyClass, keyField)
        .setRecordClass(recordClass)
        .build();
  }

  private static <K, T extends GenericRecord> Write.Builder<K, T> newBuilder(
      Class<K> keyClass, String keyField) {
    return new AutoValue_AvroSortedBucketIO_Write.Builder<K, T>()
        .setNumBuckets(SortedBucketIO.DEFAULT_NUM_BUCKETS)
        .setNumShards(SortedBucketIO.DEFAULT_NUM_SHARDS)
        .setHashType(SortedBucketIO.DEFAULT_HASH_TYPE)
        .setSorterMemoryMb(SortedBucketIO.DEFAULT_SORTER_MEMORY_MB)
        .setKeyClass(keyClass)
        .setKeyField(keyField)
        .setFilenameSuffix(DEFAULT_SUFFIX)
        .setCodec(CodecFactory.snappyCodec());
  }

  /** Write records to sorted bucket files. */
  @AutoValue
  public abstract static class Write<K, T extends GenericRecord>
      extends PTransform<PCollection<T>, WriteResult> {
    // Common
    @Nullable
    abstract BucketMetadata<K, T> getMetadata();

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

    // Avro
    @Nullable
    abstract String getKeyField();

    @Nullable
    abstract Schema getSchema();

    @Nullable
    abstract Class<T> getRecordClass();

    abstract CodecFactory getCodec();

    abstract Builder<K, T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<K, T extends GenericRecord> {
      // Common
      abstract Builder<K, T> setMetadata(BucketMetadata<K, T> metadata);

      abstract Builder<K, T> setNumBuckets(int numBuckets);

      abstract Builder<K, T> setNumShards(int numShards);

      abstract Builder<K, T> setKeyClass(Class<K> keyClass);

      abstract Builder<K, T> setHashType(HashType hashType);

      abstract Builder<K, T> setOutputDirectory(ResourceId outputDirectory);

      abstract Builder<K, T> setTempDirectory(ResourceId tempDirectory);

      abstract Builder<K, T> setFilenameSuffix(String filenameSuffix);

      abstract Builder<K, T> setSorterMemoryMb(int sorterMemoryMb);

      // Avro specific
      abstract Builder<K, T> setKeyField(String keyField);

      abstract Builder<K, T> setSchema(Schema schema);

      abstract Builder<K, T> setRecordClass(Class<T> recordClass);

      abstract Builder<K, T> setCodec(CodecFactory codec);

      abstract Write<K, T> build();
    }

    public Write<K, T> withMetadata(BucketMetadata<K, T> metadata) {
      return toBuilder().setMetadata(metadata).build();
    }

    public Write<K, T> withNumBuckets(int numBuckets) {
      return toBuilder().setNumBuckets(numBuckets).build();
    }

    public Write<K, T> withNumShards(int numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    public Write<K, T> withHashType(HashType hashType) {
      return toBuilder().setHashType(hashType).build();
    }

    public Write<K, T> to(String outputDirectory) {
      return toBuilder()
          .setOutputDirectory(FileSystems.matchNewResource(outputDirectory, true))
          .build();
    }

    public Write<K, T> withTempDirectory(String tempDirectory) {
      return toBuilder()
          .setTempDirectory(FileSystems.matchNewResource(tempDirectory, true))
          .build();
    }

    public Write<K, T> withFilenameSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    public Write<K, T> withCodec(CodecFactory codec) {
      return toBuilder().setCodec(codec).build();
    }

    @Override
    public WriteResult expand(PCollection<T> input) {
      BucketMetadata<K, T> metadata = getMetadata();
      if (metadata == null) {
        try {
          metadata =
              new AvroBucketMetadata<>(
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

      @SuppressWarnings("unchecked")
      final AvroFileOperations<T> fileOperations =
          getRecordClass() == null
              ? AvroFileOperations.of(getSchema(), getCodec())
              : (AvroFileOperations<T>)
                  AvroFileOperations.of((Class<SpecificRecordBase>) getRecordClass(), getCodec());
      SortedBucketSink<K, T> write =
          SortedBucketIO.write(
              metadata,
              outputDirectory,
              tempDirectory,
              getFilenameSuffix(),
              fileOperations,
              getSorterMemoryMb());
      return input.apply(write);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////

  @SuppressWarnings("unchecked")
  public static <KeyT> BucketedInput<KeyT, GenericRecord> source(
      TupleTag<GenericRecord> tupleTag,
      Schema schema,
      ResourceId filenamePrefix,
      String filenameSuffix) {
    return new BucketedInput<>(
        tupleTag,
        filenamePrefix,
        filenameSuffix != null ? filenameSuffix : DEFAULT_SUFFIX,
        AvroFileOperations.of(schema, CodecFactory.snappyCodec()));
  }

  public static <KeyT, ValueT extends SpecificRecordBase> BucketedInput<KeyT, ValueT> source(
      TupleTag<ValueT> tupleTag,
      Class<ValueT> recordClass,
      ResourceId filenamePrefix,
      String filenameSuffix) {
    return new BucketedInput<>(
        tupleTag,
        filenamePrefix,
        filenameSuffix != null ? filenameSuffix : DEFAULT_SUFFIX,
        AvroFileOperations.of(recordClass, CodecFactory.snappyCodec()));
  }
}
