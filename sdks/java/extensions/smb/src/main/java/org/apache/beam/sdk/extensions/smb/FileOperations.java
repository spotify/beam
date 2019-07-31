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

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.MimeTypes;

/** Abstracts IO operations sorted-bucket files. */
public abstract class FileOperations<V> implements Serializable {

  private final Compression compression;

  protected FileOperations(Compression compression) {
    this.compression = compression;
  }

  protected abstract Reader<V> createReader();

  protected abstract FileIO.Sink<V> createSink();

  public abstract Coder<V> getCoder();

  public final Iterator<V> iterator(ResourceId resourceId) throws Exception {
    final ReadableFile readableFile = toReadableFile(resourceId);

    final Reader<V> reader = createReader();
    reader.prepareRead(readableFile.open());
    return reader.iterator();
  }

  public Writer<V> createWriter() {
    return new Writer<>(createSink(), compression);
  }

  /** Sorted-bucket file reader. */
  public abstract static class Reader<V> implements Serializable {
    public abstract void prepareRead(ReadableByteChannel channel) throws Exception;

    /**
     * Reads next record in the collection. Should return null if EOF is reached. (@Todo: should we
     * have more clearly defined behavior for EOF?)
     */
    public abstract V read() throws Exception;

    public abstract void finishRead() throws Exception;

    Iterator<V> iterator() throws Exception {
      return new Iterator<V>() {
        private V next = read();

        @Override
        public boolean hasNext() {
          return next != null;
        }

        @Override
        public V next() {
          if (next == null) {
            throw new NoSuchElementException();
          }
          V result = next;
          try {
            next = read();
            if (next == null) {
              finishRead();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return result;
        }
      };
    }
  }

  /** Sorted-bucket file writer. */
  public static class Writer<V> implements Serializable, AutoCloseable {

    private final FileIO.Sink<V> sink;
    private transient WritableByteChannel channel;
    private Compression compression;

    Writer(FileIO.Sink<V> sink, Compression compression) {
      this.sink = sink;
      this.compression = compression;
    }

    public String getMimeType() {
      return MimeTypes.BINARY;
    }

    public final void prepareWrite(WritableByteChannel channel) throws Exception {
      this.channel = compression.writeCompressed(channel);
      sink.open(this.channel);
    }

    public void write(V value) throws Exception {
      sink.write(value);
    }

    @Override
    public void close() throws Exception {
      sink.flush();
      channel.close();
    }
  }

  private ReadableFile toReadableFile(ResourceId resourceId) {
    try {
      final Metadata metadata = FileSystems.matchSingleFileSpec(resourceId.toString());

      return new ReadableFile(
          metadata,
          compression == Compression.AUTO
              ? Compression.detect(resourceId.getFilename())
              : compression);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Exception fetching metadata for %s", resourceId), e);
    }
  }
}
