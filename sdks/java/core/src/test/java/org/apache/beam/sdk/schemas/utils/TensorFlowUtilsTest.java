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
import org.junit.Assert;
import org.junit.Test;
import org.tensorflow.metadata.v0.Feature;
import org.tensorflow.metadata.v0.FeatureType;
import org.tensorflow.metadata.v0.ValueCount;

public class TensorFlowUtilsTest {
  @Test
  public void testSchemaRoundTrip() {
    Schema schema = Schema.builder()
        .addByteArrayField("bytes")
        .addInt64Field("int")
        .addFloatField("float")
        .addNullableField("nullable_bytes", Schema.FieldType.BYTES)
        .addNullableField("nullable_int", Schema.FieldType.INT64)
        .addNullableField("nullable_float", Schema.FieldType.FLOAT)
        .addArrayField("array_bytes", Schema.FieldType.BYTES)
        .addArrayField("array_int", Schema.FieldType.INT64)
        .addArrayField("array_float", Schema.FieldType.FLOAT)
        .build();

    Schema copy = TensorFlowUtils.fromTfSchema(TensorFlowUtils.toTfSchema(schema));
    SchemaTestUtils.assertSchemaEquivalent(schema, copy);
  }

  @Test
  public void testRequiredField() {
    testRequiredField(Schema.FieldType.BYTES, FeatureType.BYTES);
    testRequiredField(Schema.FieldType.INT64, FeatureType.INT);
    testRequiredField(Schema.FieldType.FLOAT, FeatureType.FLOAT);
  }

  @Test
  public void testNullableField() {
    testNullableField(Schema.FieldType.BYTES, FeatureType.BYTES);
    testNullableField(Schema.FieldType.INT64, FeatureType.INT);
    testNullableField(Schema.FieldType.FLOAT, FeatureType.FLOAT);
  }

  @Test
  public void testArrayField() {
    testArrayField(Schema.FieldType.BYTES, FeatureType.BYTES);
    testArrayField(Schema.FieldType.INT64, FeatureType.INT);
    testArrayField(Schema.FieldType.FLOAT, FeatureType.FLOAT);
  }

  private void testRequiredField(Schema.FieldType fieldType, FeatureType featureType) {
    Schema.Field field = Schema.Field.of("field", fieldType);
    Feature feature = Feature.newBuilder()
        .setName("field")
        .setType(featureType)
        .setValueCount(ValueCount.newBuilder().setMin(1).setMax(1))
        .build();
    testField(field, feature);
  }

  private void testNullableField(Schema.FieldType fieldType, FeatureType featureType) {
    Schema.Field field = Schema.Field.nullable("field", fieldType);
    Feature feature = Feature.newBuilder()
        .setName("field")
        .setType(featureType)
        .setValueCount(ValueCount.newBuilder().setMin(0).setMax(1))
        .build();
    testField(field, feature);
  }

  private void testArrayField(Schema.FieldType fieldType, FeatureType featureType) {
    Schema.Field field = Schema.Field.of("field", Schema.FieldType.array(fieldType));
    Feature feature = Feature.newBuilder()
        .setName("field")
        .setType(featureType)
        .build();
    testField(field, feature);
  }

  private void testField(Schema.Field field, Feature feature) {
    Schema schema = Schema.builder().addField(field).build();
    org.tensorflow.metadata.v0.Schema tfSchema = TensorFlowUtils.toTfSchema(schema);
    org.tensorflow.metadata.v0.Schema expected = org.tensorflow.metadata.v0.Schema.newBuilder()
        .addFeature(feature)
        .build();
    Assert.assertEquals(expected, tfSchema);
  }
}
