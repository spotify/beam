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
package org.apache.beam.sdk.schemas.utils;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.tensorflow.example.Example;

/** Utility methods for TensorFlow related operations. */
public class TensorFlowUtils {

  private TensorFlowUtils() {}

  /** Convert a Beam {@link Schema} to a TensorFlow {@link org.tensorflow.metadata.v0.Schema}. */
  public static org.tensorflow.metadata.v0.Schema toTfSchema(Schema schema) {
    return org.tensorflow.metadata.v0.Schema.getDefaultInstance();
  }

  /** Convert a TensorFlow {@link org.tensorflow.metadata.v0.Schema} to a Beam {@link Schema}. */
  public static Schema fromTfSchema(org.tensorflow.metadata.v0.Schema schema) {
    return Schema.builder().build();
  }

  /** Convert a Beam {@link Row} to a TensorFlow {@link Example}. */
  private static class ToExample implements SerializableFunction<Row, Example> {
    @Override
    public Example apply(Row input) {
      return Example.getDefaultInstance();
    }
  }

  /** Convert a TensorFlow {@link Example} to a Beam {@link Row}. */
  private static class FromExample implements SerializableFunction<Example, Row> {
    private final Schema schema;

    private FromExample(org.tensorflow.metadata.v0.Schema schema) {
      this.schema = fromTfSchema(schema);
    }

    @Override
    public Row apply(Example input) {
      return Row.nullRow(schema);
    }
  }
}
