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
package org.apache.beam.sdk.nexmark;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryModel;
import org.apache.beam.sdk.nexmark.queries.Query0;
import org.apache.beam.sdk.nexmark.queries.Query0Model;
import org.apache.beam.sdk.nexmark.queries.Query1;
import org.apache.beam.sdk.nexmark.queries.Query10;
import org.apache.beam.sdk.nexmark.queries.Query11;
import org.apache.beam.sdk.nexmark.queries.Query12;
import org.apache.beam.sdk.nexmark.queries.Query1Model;
import org.apache.beam.sdk.nexmark.queries.Query2;
import org.apache.beam.sdk.nexmark.queries.Query2Model;
import org.apache.beam.sdk.nexmark.queries.Query3;
import org.apache.beam.sdk.nexmark.queries.Query3Model;
import org.apache.beam.sdk.nexmark.queries.Query4;
import org.apache.beam.sdk.nexmark.queries.Query4Model;
import org.apache.beam.sdk.nexmark.queries.Query5;
import org.apache.beam.sdk.nexmark.queries.Query5Model;
import org.apache.beam.sdk.nexmark.queries.Query6;
import org.apache.beam.sdk.nexmark.queries.Query6Model;
import org.apache.beam.sdk.nexmark.queries.Query7;
import org.apache.beam.sdk.nexmark.queries.Query7Model;
import org.apache.beam.sdk.nexmark.queries.Query8;
import org.apache.beam.sdk.nexmark.queries.Query8Model;
import org.apache.beam.sdk.nexmark.queries.Query9;
import org.apache.beam.sdk.nexmark.queries.Query9Model;
import org.apache.beam.sdk.nexmark.sinks.QueryResultsSinkFactory;
import org.apache.beam.sdk.nexmark.sinks.avro.AvroEventsSink;
import org.apache.beam.sdk.nexmark.sources.EventSourceFactory;
import org.apache.beam.sdk.nexmark.sources.pubsub.PubsubEventsGenerator;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TimestampedValue;

import org.joda.time.Duration;

/**
 * Run a single Nexmark query using a given configuration.
 */
public class NexmarkLauncher<OptionT extends NexmarkOptions> {
  /**
   * NexmarkOptions shared by all runs.
   */
  private final OptionT options;

  /**
   * Which configuration we are running.
   */
  @Nullable
  private NexmarkConfiguration configuration;

  /**
   * If in --pubsubMode=COMBINED, the pipeline result for the publisher pipeline. Otherwise null.
   */
  @Nullable
  private PipelineResult publisherResult;

  /**
   * Result for the main pipeline.
   */
  @Nullable
  private PipelineResult mainResult;

  /**
   * Query name we are running.
   */
  @Nullable
  private String queryName;

  public NexmarkLauncher(OptionT options) {
    this.options = options;
  }


  /**
   * Is this query running in streaming mode?
   */
  private boolean isStreaming() {
    return options.isStreaming();
  }

  /**
   * Return maximum number of workers.
   */
  private int maxNumWorkers() {
    return 5;
  }

  /**
   * Return a directory for logs.
   */
  private String logsDir(long now) {
    String baseFilename = options.getOutputPath();
    if (Strings.isNullOrEmpty(baseFilename)) {
      throw new RuntimeException("Missing --outputPath");
    }
    switch (options.getResourceNameMode()) {
      case VERBATIM:
        return baseFilename;
      case QUERY:
        return String.format("%s/logs_%s", baseFilename, queryName);
      case QUERY_AND_SALT:
        return String.format("%s/logs_%s_%d", baseFilename, queryName, now);
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }

  private void sinkEventsToAvro(PCollection<Event> events) {
    events.apply(AvroEventsSink.createSink(options, queryName));
  }

  /**
   * Consume {@code results}.
   */
  private void sink(PCollection<TimestampedValue<KnownSize>> results, long now) {
    if (configuration.sinkType == NexmarkUtils.SinkType.COUNT_ONLY
        || configuration.sinkType == NexmarkUtils.SinkType.DEVNULL) {
      // Avoid the cost of formatting the results.
      results.apply(queryName + ".DevNull", NexmarkUtils.devNull(queryName));
      return;
    }

    PCollection<String> formattedResults =
      results.apply(queryName + ".Format", NexmarkUtils.format(queryName));

    if (options.getLogResults()) {
      formattedResults = formattedResults.apply(queryName + ".Results.Log",
              NexmarkUtils.<String> log(queryName + ".Results"));
    }

    if (configuration.sinkType == NexmarkUtils.SinkType.AVRO) {
      NexmarkUtils.console(
          "WARNING: with --sinkType=AVRO, actual query results will be discarded.");
      return;
    }

    PTransform<PCollection<String>, ? extends POutput> sink =
        QueryResultsSinkFactory.createSink(configuration, options, queryName, now);

    formattedResults.apply(sink);
  }

  /**
   * Calculate the distribution of the expected rate of results per minute (in event time, not
   * wallclock time).
   */
  private void modelResultRates(NexmarkQueryModel model) {
    List<Long> counts = Lists.newArrayList(model.simulator().resultsPerWindow());
    Collections.sort(counts);
    int n = counts.size();
    if (n < 5) {
      NexmarkUtils.console("Query%d: only %d samples", model.configuration.query, n);
    } else {
      NexmarkUtils.console("Query%d: N:%d; min:%d; 1st%%:%d; mean:%d; 3rd%%:%d; max:%d",
                           model.configuration.query, n, counts.get(0), counts.get(n / 4),
                           counts.get(n / 2),
                           counts.get(n - 1 - n / 4), counts.get(n - 1));
    }
  }

  /**
   * Run {@code configuration} and return its performance if possible.
   */
  @Nullable
  public NexmarkPerf run(NexmarkConfiguration runConfiguration) {
    if (options.getManageResources() && !options.getMonitorJobs()) {
      throw new RuntimeException("If using --manageResources then must also use --monitorJobs.");
    }

    //
    // Setup per-run state.
    //
    checkState(configuration == null);
    checkState(queryName == null);
    configuration = runConfiguration;

    try {
      NexmarkUtils.console("Running %s", configuration.toShortString());

      if (configuration.numEvents < 0) {
        NexmarkUtils.console("skipping since configuration is disabled");
        return null;
      }

      NexmarkQuery query = createQueries().get(configuration.query);
      queryName = query.getName();

      List<NexmarkQueryModel> models = createModels();
      NexmarkQueryModel model = models.get(configuration.query);

      if (options.getJustModelResultRate()) {
        if (model == null) {
          throw new RuntimeException(String.format("No model for %s", queryName));
        }
        modelResultRates(model);
        return null;
      }

      long now = System.currentTimeMillis();
      Pipeline pipeline = newPipeline();

      if (PubsubEventsGenerator.isPublishOnly(configuration)) {
        return onlyPublishEvents(now, pipeline, query);
      }

      return executeQuery(query, model, now, pipeline);
    } finally {
      configuration = null;
      queryName = null;
    }
  }

  /**
   * Publish-only mode, send event so Pubsub, no queries are executed.
   */
  private NexmarkPerf onlyPublishEvents(long now, Pipeline pipeline, NexmarkQuery query) {
    pipeline.apply(createPubsubPublisher(now));
    return runAndGetPerf(pipeline, query);
  }

  /**
   * Get events and execute query.
   */
  private NexmarkPerf executeQuery(
      NexmarkQuery query,
      NexmarkQueryModel model,
      long now,
      Pipeline pipeline) {

    if (PubsubEventsGenerator.needPublisher(configuration)) {
      startPublisher(now);
    }

    PTransform<PBegin, PCollection<Event>> eventsSource =
        EventSourceFactory.createSource(configuration, options, queryName, now);

    PCollection<Event> events = pipeline.apply(eventsSource);

    if (options.getLogEvents()) {
      events = logEvents(events);
    }

    // Optionally sink events in Avro format.
    // (Query results are ignored).
    if (configuration.sinkType == NexmarkUtils.SinkType.AVRO) {
      sinkEventsToAvro(events);
    }

    // Apply query
    PCollection<TimestampedValue<KnownSize>> results = applyNexmarkQuery(query, events, now);

    if (options.getAssertCorrectness()) {
      assertNexmarkQueryResults(model, results);
    }

    // Output results.
    sink(results, now);

    return runAndGetPerf(pipeline, query);

  }

  private NexmarkPerf runAndGetPerf(Pipeline pipeline, NexmarkQuery query) {
    mainResult = pipeline.run();
    mainResult.waitUntilFinish(Duration.standardSeconds(configuration.streamTimeout));
    return NexmarkPerfAnalyzer
        .monitorQuery(configuration, options, query, mainResult, publisherResult);
  }

  private void startPublisher(long now) {
    Pipeline publisherPipeline = newPipeline();
    publisherPipeline.apply(createPubsubPublisher(now));
    publisherResult = publisherPipeline.run();
  }

  private PTransform<PBegin, PDone> createPubsubPublisher(long now) {
    return PubsubEventsGenerator
            .create(configuration, options, queryName, now);
  }

  private List<NexmarkQuery> createQueries() {
    return Arrays.asList(
        new Query0(configuration),
        new Query1(configuration),
        new Query2(configuration),
        new Query3(configuration),
        new Query4(configuration),
        new Query5(configuration),
        new Query6(configuration),
        new Query7(configuration),
        new Query8(configuration),
        new Query9(configuration),
        new Query10(configuration),
        new Query11(configuration),
        new Query12(configuration));
  }

  private List<NexmarkQueryModel> createModels() {
    return Arrays.asList(
        new Query0Model(configuration),
        new Query1Model(configuration),
        new Query2Model(configuration),
        new Query3Model(configuration),
        new Query4Model(configuration),
        new Query5Model(configuration),
        new Query6Model(configuration),
        new Query7Model(configuration),
        new Query8Model(configuration),
        new Query9Model(configuration),
        null,
        null,
        null);
  }

  private Pipeline newPipeline() {
    Pipeline p = Pipeline.create(options);
    NexmarkUtils.setupPipeline(configuration.coderStrategy, p);
    return p;
  }

  private PCollection<Event> logEvents(PCollection<Event> events) {
    return events.apply(
        queryName + ".Events.Log",
        NexmarkUtils.<Event> log(queryName + ".Events"));
  }

  private PCollection<TimestampedValue<KnownSize>> applyNexmarkQuery(
      NexmarkQuery query,
      PCollection<Event> events,
      long now) {

    // Query 10 logs all events to Google Cloud storage files. It could generate a lot of logs,
    // so, set parallelism. Also set the output path where to write log files.
    if (configuration.query == 10) {
      String path = null;
      if (options.getOutputPath() != null && !options.getOutputPath().isEmpty()) {
        path = logsDir(now);
      }
      ((Query10) query).setOutputPath(path);
      ((Query10) query).setMaxNumWorkers(maxNumWorkers());
    }

    // Apply query.
    return events.apply(query);
  }

  private void assertNexmarkQueryResults(
      NexmarkQueryModel model,
      PCollection<TimestampedValue<KnownSize>> results) {

    if (model == null) {
      throw new RuntimeException(String.format("No model for %s", queryName));
    }

    // We know all our streams have a finite number of elements.
    results.setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
    // If we have a finite number of events then assert our pipeline's
    // results match those of a model using the same sequence of events.
    PAssert.that(results).satisfies(model.assertionFor());
  }
}
