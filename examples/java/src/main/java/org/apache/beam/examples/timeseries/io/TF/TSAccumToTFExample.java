/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.examples.timeseries.io.tf;

import java.io.UnsupportedEncodingException;
import org.apache.beam.examples.timeseries.protos.TimeSeriesData;
import org.apache.beam.examples.timeseries.utils.TSAccums;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.Example;

/** Convert a TSAccum to a TFExample. */
@Experimental
public class TSAccumToTFExample
    extends DoFn<
        KV<TimeSeriesData.TSKey, TimeSeriesData.TSAccum>, KV<TimeSeriesData.TSKey, Example>> {

  private static final Logger LOG = LoggerFactory.getLogger(TSAccumToTFExample.class);

  @ProcessElement
  public void processElement(ProcessContext c) {

    try {
      c.output(KV.of(c.element().getKey(), TSAccums.getExampleFromAccum(c.element().getValue())));
    } catch (UnsupportedEncodingException e) {

      LOG.info("Unable to convert string to UTF-8", e);
    }
  }
}
