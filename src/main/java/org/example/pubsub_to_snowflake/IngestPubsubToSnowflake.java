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
package org.example.pubsub_to_snowflake;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeFloat;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeInteger;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeString;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.JsonToRow;

/**
 * An example that counts words in Shakespeare and includes Beam best practices.
 *
 * <p>This class, {@link IngestPubsubToSnowflake}, is the second in a series of four successively
 * more detailed 'word count' examples. You may first want to take a look at {@link
 * MinimalWordCount}. After you've looked at this example, then see the {@link DebuggingWordCount}
 * pipeline, for introduction of additional concepts.
 *
 * <p>For a detailed walkthrough of this example, see <a
 * href="https://beam.apache.org/get-started/wordcount-example/">
 * https://beam.apache.org/get-started/wordcount-example/ </a>
 *
 * <p>Basic concepts, also in the MinimalWordCount example: Reading text files; counting a
 * PCollection; writing to text files
 *
 * <p>New Concepts:
 *
 * <pre>
 *   1. Executing a Pipeline both locally and using the selected runner
 *   2. Using ParDo with static DoFns defined out-of-line
 *   3. Building a composite transform
 *   4. Defining your own pipeline options
 * </pre>
 *
 * <p>Concept #1: you can execute this pipeline either locally or using by selecting another runner.
 * These are now command-line options and not hard-coded as they were in the MinimalWordCount
 * example.
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *
 * <p>To execute this pipeline, specify a local output file (if using the {@code DirectRunner}) or
 * output prefix on a supported distributed file system.
 *
 * <pre>{@code
 * --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>The input file defaults to a public data set containing the text of of King Lear, by William
 * Shakespeare. You can override it and choose your own input with {@code --inputFile}.
 */
public class IngestPubsubToSnowflake {

  public interface PubSubToSnowflakeOptions extends SnowflakePipelineOptions {
    @Description(
        "The Cloud Pub/Sub subscription to consume from. The name should be in the format of"
            + " projects/<project-id>/subscriptions/<subscription-name>.")
    @Validation.Required
    String getInputSubscription();

    void setInputSubscription(String inputSubscription);

    @Description("The Snowflake table where the output will be saved")
    @Validation.Required
    ValueProvider<String> getOutputTable();

    void setOutputTable(ValueProvider<String> table);
  }

  @DefaultSchema(JavaFieldSchema.class)
  public static class StreamDataPojo {
    public final String id;
    public final String name;
    public final int age;
    public final float price;

    @SchemaCreate
    public StreamDataPojo(String id, String name, int age, float price) {
      this.id = id;
      this.name = name;
      this.age = age;
      this.price = price;
    }
  }

  public static SnowflakeIO.DataSourceConfiguration createSnowflakeConfiguration(
      PubSubToSnowflakeOptions options) {
    return SnowflakeIO.DataSourceConfiguration.create()
        .withUsernamePasswordAuth(options.getUsername(), options.getPassword())
        .withKeyPairRawAuth(
            options.getUsername(), options.getRawPrivateKey(), options.getPrivateKeyPassphrase())
        .withDatabase(options.getDatabase())
        .withRole(options.getRole())
        .withSchema(options.getSchema())
        .withServerName(options.getServerName());
  }

  public static void main(String[] args) throws NoSuchSchemaException {
    PubSubToSnowflakeOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToSnowflakeOptions.class);
    options.setStreaming(true);
    var inputSubscription = options.getInputSubscription();
    var dataSourceConfiguration = createSnowflakeConfiguration(options);
    var storageIntegrationName = options.getStorageIntegrationName();
    var stagingBucketName = options.getStagingBucketName();
    var snowPipe = options.getSnowPipe();
    var outputTable = options.getOutputTable();

    SnowflakeIO.UserDataMapper<StreamDataPojo> userDataMapper =
        (StreamDataPojo row) -> new Object[] {row.id, row.name, row.age, row.price};

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read PubSub Events", PubsubIO.readStrings().fromSubscription(inputSubscription))
        .apply(
            "Parse JSON rows",
            JsonToRow.withSchema(pipeline.getSchemaRegistry().getSchema(StreamDataPojo.class)))
        .apply(Convert.fromRows(StreamDataPojo.class))
        .apply(
            "Stream To Snowflake",
            SnowflakeIO.<StreamDataPojo>write()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .to(outputTable)
                .withStagingBucketName(stagingBucketName)
                .withStorageIntegrationName(storageIntegrationName)
                .withUserDataMapper(userDataMapper)
                .withSnowPipe(snowPipe)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.APPEND)
                .withTableSchema(
                    SnowflakeTableSchema.of(
                        SnowflakeColumn.of("id", SnowflakeString.of()),
                        SnowflakeColumn.of("name", SnowflakeString.of()),
                        SnowflakeColumn.of("age", SnowflakeInteger.of()),
                        SnowflakeColumn.of("price", SnowflakeFloat.of()))));

    pipeline.run();
  }
}
