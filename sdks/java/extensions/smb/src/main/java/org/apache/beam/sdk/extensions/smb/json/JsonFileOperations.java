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
package org.apache.beam.sdk.extensions.smb.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.FileOperations;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;

/** {@link FileOperations} implementation for BigQuery {@link TableRow} JSON records. */
public class JsonFileOperations extends FileOperations<TableRow> {

  public JsonFileOperations(Compression compression) {
    super(compression);
  }

  @Override
  public Reader<TableRow> createReader() {
    return new JsonReader();
  }

  @Override
  public FileIO.Sink<TableRow> createSink() {
    return new FileIO.Sink<TableRow>() {

      private final ObjectMapper objectMapper = new ObjectMapper();
      private final FileIO.Sink<String> sink = TextIO.sink();

      @Override
      public void open(WritableByteChannel channel) throws IOException {
        sink.open(channel);
      }

      @Override
      public void write(TableRow element) throws IOException {
        sink.write(objectMapper.writeValueAsString(element));
      }

      @Override
      public void flush() throws IOException {
        sink.flush();
      }
    };
  }

  @Override
  public Coder<TableRow> getCoder() {
    return TableRowJsonCoder.of();
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class JsonReader extends FileOperations.Reader<TableRow> {

    private transient ObjectMapper objectMapper;
    private transient BufferedReader reader;

    @Override
    public void prepareRead(ReadableByteChannel channel) throws Exception {
      objectMapper = new ObjectMapper();
      reader =
          new BufferedReader(
              new InputStreamReader(Channels.newInputStream(channel), Charset.defaultCharset()));
    }

    @Override
    public TableRow read() throws Exception {
      String next = reader.readLine();
      if (next == null) {
        return null;
      }

      return objectMapper.readValue(next, TableRow.class);
    }

    @Override
    public void finishRead() throws Exception {
      reader.close();
    }
  }
}
