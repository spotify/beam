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
package org.apache.beam.sdk.io;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteShardsIntoFiles<ShardedKeyT, UserT, DestinationT>
    extends PTransform<
        PCollection<KV<ShardedKeyT, Iterable<UserT>>>, WriteFilesResult<DestinationT>> {
  private static final Logger LOG = LoggerFactory.getLogger(WriteShardsIntoFiles.class);

  @Override
  public WriteFilesResult<DestinationT> expand(
      PCollection<KV<ShardedKeyT, Iterable<UserT>>> input) {
    return null;
  }

  public abstract static class WriteShardsIntoTempFilesFn<ShardedKeyT, UserT, DestinationT, OutputT>
      extends DoFn<KV<ShardedKeyT, Iterable<UserT>>, KV<KV<DestinationT, ResourceId>, ResourceId>> {
    private final FileBasedSink.WriteOperation<DestinationT, OutputT> writeOperation;

    protected WriteShardsIntoTempFilesFn(
        FileBasedSink.WriteOperation<DestinationT, OutputT> writeOperation) {
      this.writeOperation = writeOperation;
    }

    protected abstract ResourceId getDestination(
        ProcessContext c, BoundedWindow window, DestinationT destination) throws Exception;

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      getDynamicDestinations().setSideInputAccessorFromProcessContext(c);
      // Since we key by a 32-bit hash of the destination, there might be multiple destinations
      // in this iterable. The number of destinations is generally very small (1000s or less), so
      // there will rarely be hash collisions.
      Map<DestinationT, FileBasedSink.Writer<DestinationT, OutputT>> writers = Maps.newHashMap();
      for (UserT input : c.element().getValue()) {
        DestinationT destination = getDynamicDestinations().getDestination(input);
        FileBasedSink.Writer<DestinationT, OutputT> writer = writers.get(destination);
        if (writer == null) {
          String uuid = UUID.randomUUID().toString();
          LOG.info(
              "Opening writer {} for window {} pane {} destination {}",
              uuid,
              window,
              c.pane(),
              destination);
          writer = writeOperation.createWriter();
          writer.setDestination(destination);
          writer.open(uuid);
          writers.put(destination, writer);
        }
        writeOrClose(writer, getDynamicDestinations().formatRecord(input));
      }

      // Close all writers.
      for (Map.Entry<DestinationT, FileBasedSink.Writer<DestinationT, OutputT>> entry :
          writers.entrySet()) {
        FileBasedSink.Writer<DestinationT, OutputT> writer = entry.getValue();
        try {
          // Close the writer; if this throws let the error propagate.
          writer.close();
        } catch (Exception e) {
          // If anything goes wrong, make sure to delete the temporary file.
          writer.cleanup();
          throw e;
        }

        ResourceId destination = getDestination(c, window, entry.getKey());
        c.output(KV.of(KV.of(entry.getKey(), writer.getOutputFile()), destination));
      }
    }

    @SuppressWarnings("unchecked")
    private FileBasedSink.DynamicDestinations<UserT, DestinationT, OutputT>
        getDynamicDestinations() {
      return (FileBasedSink.DynamicDestinations<UserT, DestinationT, OutputT>)
          writeOperation.getSink().getDynamicDestinations();
    }

    private void writeOrClose(FileBasedSink.Writer<DestinationT, OutputT> writer, OutputT t)
        throws Exception {
      try {
        writer.write(t);
      } catch (Exception e) {
        try {
          writer.close();
          // If anything goes wrong, make sure to delete the temporary file.
          writer.cleanup();
        } catch (Exception closeException) {
          if (closeException instanceof InterruptedException) {
            // Do not silently ignore interrupted state.
            Thread.currentThread().interrupt();
          }
          // Do not mask the exception that caused the write to fail.
          e.addSuppressed(closeException);
        }
        throw e;
      }
    }
  }

  public static class FinalizeTempFiles<UserT, OutputT, DestinationT>
    extends PTransform<PCollection<KV<KV<DestinationT, ResourceId>, ResourceId>>, WriteFilesResult<DestinationT>> {
    @Nullable private final PCollectionView<Integer> numShardsView;
    @Nullable  private final ValueProvider<Integer> numShardsProvider;
    private final boolean windowedWrites;
    private final List<PCollectionView<?>> sideInputs;
    private final FileBasedSink.WriteOperation<DestinationT, OutputT> writeOperation;
    private final Coder<DestinationT> destinationCoder;

    public FinalizeTempFiles(@Nullable PCollectionView<Integer> numShardsView,
                             @Nullable ValueProvider<Integer> numShardsProvider,
                             boolean windowedWrites,
                             List<PCollectionView<?>> sideInputs,
                             FileBasedSink.WriteOperation<DestinationT, OutputT> writeOperation,
                             Coder<DestinationT> destinationCoder) {
      this.numShardsView = numShardsView;
      this.numShardsProvider = numShardsProvider;
      this.windowedWrites = windowedWrites;
      this.sideInputs = sideInputs;
      this.writeOperation = writeOperation;
      this.destinationCoder = destinationCoder;
    }

    @Override
    public WriteFilesResult<DestinationT> expand(PCollection<KV<KV<DestinationT, ResourceId>, ResourceId>> input) {
      return input
          .apply("GatherTempFileResults", new GatherResults<>())
          .apply("FinalizeTempFileBundles", new FinalizeTempFileBundles());
    }

    private class GatherResults<ResultT>
        extends PTransform<PCollection<ResultT>, PCollection<List<ResultT>>> {
      @Override
      public PCollection<List<ResultT>> expand(PCollection<ResultT> input) {
        if (windowedWrites) {
          // Reshuffle the results to make them stable against retries.
          // Use a single void key to maximize size of bundles for finalization.
          return input
              .apply("Add void key", WithKeys.of((Void) null))
              .apply("Reshuffle", Reshuffle.of())
              .apply("Drop key", Values.create())
              .apply("Gather bundles", ParDo.of(new GatherBundlesPerWindowFn<>()))
              .setCoder(ListCoder.of(input.getCoder()))
              // Reshuffle one more time to stabilize the contents of the bundle lists to finalize.
              .apply(Reshuffle.viaRandomKey());
        } else {
          // Pass results via a side input rather than reshuffle, because we need to get an empty
          // iterable to finalize if there are no results.
          return input
              .getPipeline()
              .apply(
                  Reify.viewInGlobalWindow(
                      input.apply(View.asList()), ListCoder.of(input.getCoder())));
        }
      }
    }

    private static class GatherBundlesPerWindowFn<T> extends DoFn<T, List<T>> {
      @Nullable private transient Multimap<BoundedWindow, T> bundles = null;

      @StartBundle
      public void startBundle() {
        bundles = ArrayListMultimap.create();
      }

      @ProcessElement
      public void process(ProcessContext c, BoundedWindow w) {
        bundles.put(w, c.element());
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext c) throws Exception {
        for (BoundedWindow w : bundles.keySet()) {
          c.output(Lists.newArrayList(bundles.get(w)), w.maxTimestamp(), w);
        }
      }
    }

    private class FinalizeTempFileBundles
        extends PTransform<PCollection<List<KV<KV<DestinationT, ResourceId>, ResourceId>>>, WriteFilesResult<DestinationT>> {
      @Override
      public WriteFilesResult<DestinationT> expand(
          PCollection<List<KV<KV<DestinationT, ResourceId>, ResourceId>>> input) {

        List<PCollectionView<?>> finalizeSideInputs = Lists.newArrayList(sideInputs);
        if (numShardsView != null) {
          finalizeSideInputs.add(numShardsView);
        }
        PCollection<KV<DestinationT, String>> outputFilenames =
            input
                .apply("Finalize", ParDo.of(new FinalizeFn()).withSideInputs(finalizeSideInputs))
                .setCoder(KvCoder.of(destinationCoder, StringUtf8Coder.of()))
                // Reshuffle the filenames to make sure they are observable downstream
                // only after each one is done finalizing.
                .apply(Reshuffle.viaRandomKey());

        TupleTag<KV<DestinationT, String>> perDestinationOutputFilenamesTag =
            new TupleTag<>("perDestinationOutputFilenames");
        return WriteFilesResult.in(
            input.getPipeline(), perDestinationOutputFilenamesTag, outputFilenames);
      }

      private class FinalizeFn
          extends DoFn<List<KV<KV<DestinationT, ResourceId>, ResourceId>>, KV<DestinationT, String>> {
        @ProcessElement
        public void process(ProcessContext c) throws Exception {
          getDynamicDestinations().setSideInputAccessorFromProcessContext(c);
          @Nullable Integer fixedNumShards;
          if (numShardsView != null) {
            fixedNumShards = c.sideInput(numShardsView);
          } else if (numShardsProvider != null) {
            fixedNumShards = numShardsProvider.get();
          } else {
            fixedNumShards = null;
          }
          List<KV<KV<DestinationT, ResourceId>, ResourceId>> fileResults = c.element();
          LOG.info("Finalizing {} file results", fileResults.size());
          DestinationT defaultDest = getDynamicDestinations().getDefaultDestination();
          List<KV<KV<DestinationT, ResourceId>, ResourceId>> resultsToFinalFilenames =
              fileResults.isEmpty()
                  ? writeOperation.finalizeDestination(
                  defaultDest, GlobalWindow.INSTANCE, fixedNumShards, Collections.emptyList())
                  : fileResults;
          writeOperation.moveToOutputFiles(resultsToFinalFilenames);
          for (KV<KV<DestinationT, ResourceId>, ResourceId> entry : resultsToFinalFilenames) {
            KV<DestinationT, ResourceId> res = entry.getKey();
            c.output(KV.of(res.getKey(), entry.getValue().toString()));
          }
        }
      }

      @SuppressWarnings("unchecked")
      private FileBasedSink.DynamicDestinations<UserT, DestinationT, OutputT>
      getDynamicDestinations() {
        return (FileBasedSink.DynamicDestinations<UserT, DestinationT, OutputT>)
            writeOperation.getSink().getDynamicDestinations();
      }
    }
  }
}
