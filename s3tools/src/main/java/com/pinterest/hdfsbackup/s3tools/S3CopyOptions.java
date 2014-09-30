package com.pinterest.hdfsbackup.s3tools;

import com.pinterest.hdfsbackup.options.OptionWithArg;
import com.pinterest.hdfsbackup.options.Options;
import com.pinterest.hdfsbackup.options.SimpleOption;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * This class encapsulates options user provides through either "--opt=value", or,
 * through system properties like "-Dproperty=value".
 *
 * Created by shawn on 8/26/14.
 */
public class S3CopyOptions {
  private static final Log log = LogFactory.getLog(S3CopyOptions.class);
  // Below options are provided at cmd line through "--opt=value"
  public String srcPath = null;
  public String destPath = null;
  public boolean helpDefined = false;
  public boolean verbose = false;

  // Options below are specified through system properties "-Dproperty=value".
  // These opts are needed by mapper/reducer at remote nodes, so they have
  // to be passed through system Configuration.
  public boolean verifyChecksum = true;
  public boolean useMultipart = true;
  public int queueSize = 2000;
  public int workerThreads = 10;
  public int maxInflightParts = 2;
  public long chunkSize = 1024L * 1024 * 30;
  public boolean useInterimFiles = false;
  // a ',' separated list of dirs to use as interim stage area for multi-part ops.
  public String interimDirs = "";
  public String manifestFilename = null;
  // "compareChecksum" is used only by compare-dir tool.
  public boolean compareChecksum = false;

  public S3CopyOptions() { }

  /**
   * Some options are specified through command line config "-Dproperty=value"
   * @param conf
   */
  public void populateFromConfiguration(Configuration conf) {
    // Each task has a queue to store file pairs to process.
    this.queueSize = conf.getInt("s3copy.queueSize", 2000);
    // Each reducer spawns this many worker threads.
    this.workerThreads = conf.getInt("s3copy.workerThreads", 10);
    // If use multipart or not. It's hardwired to always be true. Don't overwrite it.
    this.useMultipart = conf.getBoolean("s3copy.multipart", true);
    // Multipart chunk size. This size should match with the multi-part upload
    // chunk size for better performance.
    this.chunkSize = conf.getInt("s3copy.chunkSizeMB", 30) * 1024L * 1024;
    // Issue this many multi-part request on the wire.
    this.maxInflightParts = conf.getInt("s3copy.maxInflightParts", 2);
    // Whether to verify checksum during transmit.
    this.verifyChecksum = conf.getBoolean("s3copy.checksum", true);
    // During multi-part download, you can choose to put intermediate chunks
    // in a temp dir before re-assemble them to final file, such that
    // in-flight parts can complete out of order.
    // This is usually faster than waiting for parts to complete in order.
    this.useInterimFiles = conf.getBoolean("s3copy.useInterimFiles", false);
    // When comparing dir, also compare file checksums.
    this.compareChecksum = conf.getBoolean("s3copy.compareChecksum", false);
  }

  public void showCopyOptions() {
    StringBuilder sb = new StringBuilder();
    sb.append("S3Copy options: \n")
      .append("\tsource: " + this.srcPath + "\n")
      .append("\ttarget: " + this.destPath + "\n")
      .append(String.format("\tuse multipart:           %s\n", this.useMultipart))
      .append(String.format("\tverify checksum:         %s\n", this.verifyChecksum))
      .append(String.format("\tmultipart chunk size:    %d\n", this.chunkSize))
      .append(String.format("\tqueue size:              %d\n", this.queueSize))
      .append(String.format("\tworker threads per task: %d\n", this.workerThreads))
      .append(String.format("\tmax inflight parts:      %d\n", this.maxInflightParts))
      .append(String.format("\tuse interim files:       %s\n", this.useInterimFiles))
      .append(String.format("\tcompare checksum:        %s\n", this.compareChecksum));

    log.info(sb.toString());
  }

  public S3CopyOptions(String args[]) {
    Options options = new Options();

    SimpleOption helpOption = options.noArg("--help", "Print help text");
    SimpleOption verbose = options.noArg("--verbose", "be verbose");
    OptionWithArg srcOption = options.withArg("--src", "Source directory");
    OptionWithArg destOption = options.withArg("--dest", "Dest directory");
    OptionWithArg manifestFilenameOption = options.withArg("--manifest",
                                                           "a file containing all files to copy");

    options.parseArguments(args);
    if (helpOption.defined()) {
      log.info(options.helpText());
      this.helpDefined = true;
      return;
    }
    if (!manifestFilenameOption.defined()) {
      srcOption.require();
      //destOption.require();
    } else {
      if (srcOption.defined() || destOption.defined()) {
        log.info("Error: Cannot specify --manifest and --src/dest at the same time.");
        System.exit(1);
      }
    }

    if (verbose.defined()) {
      this.verbose = true;
    }
    if (srcOption.defined()) {
      srcPath = srcOption.getValue();
      if (srcPath.endsWith("/")) {
        srcPath = srcPath.substring(0, srcPath.length() - 1);
      }
    }
    if (destOption.defined()) {
      destPath = destOption.getValue();
      if (destPath.endsWith("/")) {
        destPath = destPath.substring(0, destPath.length() - 1);
      }
    }
    if (manifestFilenameOption.defined()) {
      this.manifestFilename = manifestFilenameOption.getValue();
    }
  }

}

