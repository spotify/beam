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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO;
import org.apache.beam.sdk.extensions.smb.JsonSortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketIO;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource;
import org.apache.beam.sdk.io.AvroGeneratedUser;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;

/** Benchmark of {@link SortedBucketSource} using data generated by {@link SinkBenchmark}. */
public class SourceBenchmark {

  /** SourceOptions. */
  public interface SourceOptions extends PipelineOptions {
    String getAvroSource();

    void setAvroSource(String value);

    String getJsonSource();

    void setJsonSource(String value);
  }

  public static void main(String[] args) throws IOException {
    final SourceOptions sourceOptions =
        PipelineOptionsFactory.fromArgs(args).as(SourceOptions.class);
    final Pipeline pipeline = Pipeline.create(sourceOptions);

    TupleTag<AvroGeneratedUser> lhsTag = new TupleTag<>();
    TupleTag<TableRow> rhsTag = new TupleTag<>();
    final SortedBucketIO.CoGbk<String> read =
        SortedBucketIO.read(String.class)
            .of(
                AvroSortedBucketIO.read(lhsTag, AvroGeneratedUser.class)
                    .from(sourceOptions.getAvroSource()))
            .and(JsonSortedBucketIO.read(rhsTag).from(sourceOptions.getJsonSource()));

    pipeline
        .apply(read)
        .apply(
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, KV<String, KV<AvroGeneratedUser, TableRow>>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    final KV<String, CoGbkResult> kv = c.element();
                    final CoGbkResult result = kv.getValue();
                    for (AvroGeneratedUser l : result.getAll(lhsTag)) {
                      for (TableRow r : result.getAll(rhsTag)) {
                        c.output(KV.of(kv.getKey(), KV.of(l, r)));
                      }
                    }
                  }
                }))
        .setCoder(
            KvCoder.of(
                StringUtf8Coder.of(),
                KvCoder.of(AvroCoder.of(AvroGeneratedUser.class), TableRowJsonCoder.of())))
        .apply(Count.globally())
        .apply(
            MapElements.into(TypeDescriptors.longs())
                .via(
                    c -> {
                      System.out.println("Global count = " + c);
                      return c;
                    }));

    long startTime = System.currentTimeMillis();
    State state = pipeline.run().waitUntilFinish();
    System.out.println(
        String.format(
            "SourceBenchmark finished with state %s in %d ms",
            state, System.currentTimeMillis() - startTime));
  }
}
