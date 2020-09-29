package com.ncr.ly185009.utility;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This pipeline decompresses file(s) from Google Cloud Storage and re-uploads them to a destination location.
 *
 * <p><b>Parameters</b>
 *
 * <p>The {@code --inputFilePattern} parameter specifies a file glob to process. Files found can be
 * expressed in the following formats:
 *
 * <pre>
 * --inputFilePattern=gs://bucket-name/compressed-dir/*
 * --inputFilePattern=gs://bucket-name/compressed-dir/demo*.gz
 * </pre>
 *
 * <p>The {@code --outputDirectory} parameter can be expressed in the following formats:
 *
 * <pre>
 * --outputDirectory=gs://bucket-name
 * --outputDirectory=gs://bucket-name/decompressed-dir
 * </pre>
 *
 * <p>The {@code --outputFailureFile} parameter indicates the file to write the names of the files
 * which failed decompression and their associated error messages. This file can then be used for subsequent processing
 * by another process outside of Dataflow (e.g. send an email with the failures, etc.). If there are no failures, the
 * file will still be created but will be empty. The failure file structure contains both the file that caused the error
 * and the error message in CSV format. The file will contain one header row and two columns (Filename, Error). The
 * filename output to the failureFile will be the full path of the file for ease of debugging.
 *
 * <pre>
 * --outputFailureFile=gs://bucket-name/decompressed-dir/failed.csv
 * </pre>
 *
 * <p>Example Output File:
 *
 * <pre>
 * Filename,Error
 * gs://docs-demo/compressedFile.gz, File is malformed or not compressed in BZIP2 format.
 * </pre>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.BulkDecompressor \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 * --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 * --runner=DataflowRunner \
 * --inputFilePattern=gs://${PROJECT_ID}/compressed-dir/*.gz \
 * --outputDirectory=gs://${PROJECT_ID}/decompressed-dir \
 * --outputFailureFile=gs://${PROJECT_ID}/decompressed-dir/failed.csv"
 * </pre>
 */
public class BulkDecompressor {

    /**
     * The tag used to identify the main output of the {@link Decompress} DoFn.
     */
    @VisibleForTesting
    static final TupleTag<String> DECOMPRESS_MAIN_OUT_TAG = new TupleTag<String>() {
    };

    /**
     * The tag used to identify the dead-letter sideOutput of the {@link Decompress} DoFn.
     */
    @VisibleForTesting
    static final TupleTag<KV<String, String>> DEADLETTER_TAG = new TupleTag<KV<String, String>>() {
    };

    /**
     * The logger to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(BulkDecompressor.class);

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not wait for it's
     * execution to finish. If blocking execution is required, use the {@link BulkDecompressor#run(Options)} method to
     * start the pipeline and invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {

        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(Options.class);

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the pipeline is
     * finished before returning. Invoke {@code result.waitUntilFinish()} on the result object to block until the
     * pipeline is finished running if blocking programmatic execution is required.
     *
     * @param options The execution options.
     *
     * @return The pipeline result.
     */
    public static PipelineResult run(Options options) {

        /*
         * Steps:
         *   1) Find all files matching the input pattern
         *   2) Decompress the files found and output them to the output directory
         *   3) Write any errors to the failure output file
         */

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Run the pipeline over the work items.
        PCollectionTuple decompressOut =
                pipeline
                        .apply("MatchFile(s)", FileIO
                                .match()
                                .filepattern(options.getInputFilePattern()))
                        .apply(
                                "DecompressFile(s)",
                                ParDo
                                        .of(new Decompress())
                                        .withOutputTags(DECOMPRESS_MAIN_OUT_TAG, TupleTagList.of(DEADLETTER_TAG)));

        decompressOut
                .get(DECOMPRESS_MAIN_OUT_TAG)
                .apply("String ", JavascriptTextTransformer.TransformTextViaJavascript
                        .newBuilder()
                        .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                        .setFunctionName(options.getJavascriptTextTransformFunctionName())
                        .build())
                .apply(
                        "Write to PubSub",
                        PubsubIO
                                .writeStrings()
                                .to(options.getOutputTopic()));
        return pipeline.run();
    }

    /**
     * The {@link Options} class provides the custom execution options passed by the executor at the command-line.
     */
    public interface Options extends PipelineOptions, PubsubConverters.PubsubWriteOptions,
            JavascriptTextTransformer.JavascriptTextTransformerOptions {
        @Description("The input file pattern to read from (e.g. gs://bucket-name/compressed/*.gz)")
        @Validation.Required
        ValueProvider<String> getInputFilePattern();

        void setInputFilePattern(ValueProvider<String> value);
/*
        @Description("The output location to write to (e.g. gs://bucket-name/decompressed)")
        @Validation.Required
        ValueProvider<String> getOutputDirectory();

        void setOutputDirectory(ValueProvider<String> value);*/

        @Description("The name of the topic which data should be published to. "
                + "The name should be in the format of projects/<project-id>/topics/<topic-name>.")
        @Validation.Required
        ValueProvider<String> getOutputTopic();

        void setOutputTopic(ValueProvider<String> value);

     /*   @Description(
                "The output file to write failures during the decompression process "
                        + "(e.g. gs://bucket-name/decompressed/failed.txt). The contents will be one line for "
                        + "each file which failed decompression. Note that this parameter will "
                        + "allow the pipeline to continue processing in the event of a failure.")
        @Validation.Required
        ValueProvider<String> getOutputFailureFile();

        void setOutputFailureFile(ValueProvider<String> value);*/
    }

    /**
     * Performs the decompression of an object on Google Cloud Storage and uploads the decompressed object back to a
     * specified destination location.
     */
    @SuppressWarnings("serial")
    public static class Decompress extends DoFn<MatchResult.Metadata, String> {

        //private final ValueProvider<String> destinationLocation;

        Decompress() {
            //  this.destinationLocation = null ; //destinationLocation;
        }

        @DoFn.ProcessElement
        public void processElement(ProcessContext context) {
            org.apache.beam.sdk.io.fs.ResourceId inputFile = context
                    .element()
                    .resourceId();
            try (ZipInputStream zis =
                    new ZipInputStream(Channels.newInputStream(FileSystems.open(
                            inputFile)), StandardCharsets.UTF_8)) {
                ZipEntry entry = zis.getNextEntry();
                LOG.info("Reading Zip input files");
                int i = 0;
                StringBuilder s = new StringBuilder();
                while ((entry = zis.getNextEntry()) != null) {
                    LOG.info(" new file # {} ; Name: {}", i, entry.getName());
                    byte[] buffer = new byte[150000];
                    int read = 0;
                    while ((read = zis.read(buffer, 0, buffer.length)) >= 0) {
                        s.append(new String(buffer, 0, read));
                    }
                    //LOG.info("done file {} {}", ++i, s.toString());
                    context.output(s.toString());
                    s.setLength(0);
                }
                // Rename the temp file to the output file.
            } catch (IOException e) {
                String msg = e.getMessage();

                LOG.error("Error occurred during decompression of {}", inputFile.toString(), e);
                context.output(DEADLETTER_TAG, KV.of(inputFile.toString(), e.getMessage()));
            }
        }
    }
}
