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

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.FileOperations;
import org.apache.beam.sdk.extensions.smb.JsonSortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.TensorFlowBucketIO;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.tensorflow.example.Example;

/** Test public API access level. */
public class Test {
  public static void main(String[] args)
      throws Coder.NonDeterministicException, CannotProvideCoderException, IOException {
    // public API
    AvroSortedBucketIO.write(String.class, "name", AvroGeneratedUser.class).to("avro");
    JsonSortedBucketIO.write(String.class, "name").to("json");
    TensorFlowBucketIO.write(String.class, "name").to("tf");

    TupleTag<AvroGeneratedUser> avro = new TupleTag<>("avro");
    TupleTag<TableRow> json = new TupleTag<>("json");
    TupleTag<Example> tf = new TupleTag<>("tf");
    SortedBucketIO.read(String.class)
        .of(AvroSortedBucketIO.read(avro, AvroGeneratedUser.class).from("avro"))
        .and(JsonSortedBucketIO.read(json).from("json"))
        .and(TensorFlowBucketIO.read(tf).from("tf"));

    // extendable API
    new SortedBucketSink<>(
        new MyMetadata(8, 1, String.class, HashType.MURMUR3_32),
        FileSystems.matchNewResource("output", true),
        FileSystems.matchNewResource("temp", true),
        ".avro",
        new MyFileOperation(),
        1);

    new SortedBucketSource<>(
        String.class,
        Collections.singletonList(
            new BucketedInput<>(
                new TupleTag<>(),
                FileSystems.matchSingleFileSpec("in").resourceId(),
                ".avro",
                new MyFileOperation())));
  }

  private static class AvroAutoGenClass extends SpecificRecordBase {
    @Override
    public Schema getSchema() {
      return null;
    }

    @Override
    public Object get(int field) {
      return null;
    }

    @Override
    public void put(int field, Object value) {}
  }

  public static void test() {
    Pipeline p = null;

    final TupleTag<AvroAutoGenClass> avroTag = new TupleTag<>();
    final TupleTag<TableRow> jsonTag = new TupleTag<>();
    final TupleTag<Example> tfTag = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
        p.apply(
            SortedBucketIO.read(String.class)
                .of(
                    AvroSortedBucketIO.read(avroTag, AvroAutoGenClass.class)
                        .from("/path/to/avro")
                        .withSuffix(".avro")
                        .withCodec(CodecFactory.snappyCodec()))
                .and(
                    JsonSortedBucketIO.read(jsonTag)
                        .from("/path/to/json")
                        .withSuffix(".json")
                        .withCompression(Compression.AUTO))
                .and(
                    TensorFlowBucketIO.read(tfTag)
                        .from("/path/to/tf")
                        .withSuffix(".tfrecord")
                        .withCompression(Compression.AUTO)));
  }

  private static class MyMetadata extends BucketMetadata<String, String> {
    private MyMetadata(int numBuckets, int numShards, Class<String> keyClass, HashType hashType)
        throws CannotProvideCoderException, Coder.NonDeterministicException {
      super(BucketMetadata.CURRENT_VERSION, numBuckets, numShards, keyClass, hashType);
    }

    @Override
    public String extractKey(String value) {
      return null;
    }
  }

  private static class MyFileOperation extends FileOperations<String> {

    private MyFileOperation() {
      super(Compression.UNCOMPRESSED, MimeTypes.BINARY);
    }

    @Override
    protected Reader<String> createReader() {
      return null;
    }

    @Override
    protected FileIO.Sink<String> createSink() {
      return null;
    }

    @Override
    public Coder<String> getCoder() {
      return null;
    }
  }
}
