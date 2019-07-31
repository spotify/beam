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
package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Sorted-bucket files are {@code PCollection<V>}s written with {@link SortedBucketSink} that can be
 * efficiently merged without shuffling with {@link SortedBucketSource}. When writing, values are
 * grouped by key into buckets, sorted by key within a bucket, and written to files. When reading,
 * key-values in matching buckets are read in a merge-sort style, reducing shuffle.
 */
public class SortedBucketIO {

  /** Builder for a {@link SortedBucketSource} for a given key class. */
  public static <K> ReadBuilder<K, ?, ?> read(Class<K> keyClass) {
    return new ReadBuilder<>(keyClass);
  }

  /**
   * Builder for a typed two-way sorted-bucket source.
   *
   * @param <K> the type of the keys
   * @param <V1> the type of the left-hand side values
   * @param <V2> the type of the right-hand side values
   */
  public static class ReadBuilder<K, V1, V2> {
    private Class<K> keyClass;
    private BucketedInput<K, V1> lhs;
    private BucketedInput<K, V2> rhs;

    private ReadBuilder(Class<K> keyClass) {
      this.keyClass = keyClass;
    }

    private ReadBuilder(Class<K> keyClass, BucketedInput<K, V1> lhs) {
      this(keyClass);
      this.lhs = lhs;
    }

    public <V> ReadBuilder<K, V, ?> of(BucketedInput<K, V> lhs) {
      final ReadBuilder<K, V, ?> builderCopy = new ReadBuilder<>(keyClass);

      builderCopy.lhs = lhs;
      return builderCopy;
    }

    public <W> ReadBuilder<K, V1, W> and(BucketedInput<K, W> rhs) {
      final ReadBuilder<K, V1, W> builderCopy = new ReadBuilder<>(keyClass, lhs);

      builderCopy.rhs = rhs;
      return builderCopy;
    }

    public SortedBucketSource<K> build() {
      return new SortedBucketSource<>(ImmutableList.of(lhs, rhs), keyClass);
    }
  }

  public static <K, V> SortedBucketSink<K, V> write(
      BucketMetadata<K, V> metadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      String filenameSuffix,
      FileOperations<V> fileOperations) {
    return new SortedBucketSink<>(
        metadata,
        new SMBFilenamePolicy(outputDirectory, filenameSuffix),
        fileOperations,
        tempDirectory);
  }
}
