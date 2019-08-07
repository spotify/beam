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
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.tensorflow.example.Example;
import org.tensorflow.metadata.v0.Feature;
import org.tensorflow.metadata.v0.FeatureType;
import org.tensorflow.metadata.v0.FixedShape;
import org.tensorflow.metadata.v0.ValueCount;

/** Utility methods for TensorFlow related operations. */
public class TensorFlowUtils {

  private TensorFlowUtils() {}

  /** Convert a Beam {@link Schema} to a TensorFlow {@link org.tensorflow.metadata.v0.Schema}. */
  public static org.tensorflow.metadata.v0.Schema toTfSchema(Schema schema) {
    org.tensorflow.metadata.v0.Schema.Builder builder =
        org.tensorflow.metadata.v0.Schema.newBuilder();
    schema.getFields().stream().map(TensorFlowUtils::toFeature).forEach(builder::addFeature);
    return builder.build();
  }

  /** Convert a TensorFlow {@link org.tensorflow.metadata.v0.Schema} to a Beam {@link Schema}. */
  public static Schema fromTfSchema(org.tensorflow.metadata.v0.Schema schema) {
    Schema.Builder builder = Schema.builder();
    schema.getFeatureList().stream()
        .map(TensorFlowUtils::toField)
        .forEach(builder::addField);
    // According to schema.proto, SparseFeature.{IndexFeature,ValueFeature} should be a reference to
    // an existing feature in the schema.
    return builder.build();
  }

  private static Feature toFeature(Field field) {
    String name = field.getName();
    Schema.FieldType fieldType = field.getType();
    boolean isArray = fieldType.getTypeName() == Schema.TypeName.ARRAY;
    boolean isNullable = fieldType.getNullable();

    Feature.Builder builder = Feature.newBuilder().setName(name);
    Schema.TypeName typeName = isArray
        ? fieldType.getCollectionElementType().getTypeName()
        : fieldType.getTypeName();

    switch (typeName) {
      case BYTES:
        builder.setType(FeatureType.BYTES);
        break;
      case INT64:
        builder.setType(FeatureType.INT);
        break;
      case FLOAT:
        builder.setType(FeatureType.FLOAT);
        break;
      default:
        throw new IllegalStateException(
            String.format("Field type %s not supported for field %s", typeName, name));
    }

    Preconditions.checkState(!(isArray && isNullable),
        String.format("Field %s is both array and nullable", name));

    if (isNullable) {
      // nullable
      builder.setValueCount(ValueCount.newBuilder().setMin(0).setMax(1));
    } else if (!isArray){
      // required
      builder.setValueCount(ValueCount.newBuilder().setMin(1).setMax(1));
    }
    // repeated, no-op
    return builder.build();
  }

  private static Field toField(Feature feature) {
    String name = feature.getName();
    Schema.FieldType fieldType;

    // Map FeatureType to those supported by TF.Example
    // https://github.com/tensorflow/tensorflow/tree/master/tensorflow/core/example
    switch (feature.getType()) {
      case BYTES:
        fieldType = Schema.FieldType.BYTES;
        break;
      case INT:
        fieldType = Schema.FieldType.INT64;
        break;
      case FLOAT:
        fieldType = Schema.FieldType.FLOAT;
        break;
      default:
        throw new IllegalStateException("Feature type " + feature.getType() +
            " not supported for feature " + name);
    }


    Field field = null;
    switch (feature.getShapeTypeCase()) {
      case SHAPE:
        FixedShape shape = feature.getShape();
        if (shape.getDimCount() == 1 && shape.getDim(0).getSize() == 1) {
          // required
          field = Field.of(name, fieldType);
        } else {
          // repeated
          field = Field.of(name, Schema.FieldType.array(fieldType));
        }
        break;
      case VALUE_COUNT:
        ValueCount valueCount = feature.getValueCount();
        if (valueCount.getMin() == 0 && valueCount.getMax() == 1) {
          // nullable
          field = Field.nullable(name, fieldType);
        } else if (valueCount.getMin() == 1 && valueCount.getMax() == 1) {
          // required
          field = Field.of(name, fieldType);
        } else {
          // repeated
          field = Field.of(name, Schema.FieldType.array(fieldType));
        }
        break;
      case SHAPETYPE_NOT_SET:
        // repeated
        field = Field.of(name, Schema.FieldType.array(fieldType));
        break;
    }
    // TODO: description
    return field;
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
