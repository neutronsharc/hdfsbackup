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
  public int queueSize = 100;
  public int workerThreads = 10;
  public long chunkSize = 1024L * 1024 * 32;

  public S3CopyOptions() { }

  /**
   * Some options are specified through command line config "-Dproperty=value"
   * @param conf
   */
  public void populateFromConfiguration(Configuration conf) {
    // Each reducer has a queue to store file pairs to process.
    this.queueSize = conf.getInt("s3copy.queuesize", 100);
    // Each reducer spawns this many worker threads.
    this.workerThreads = conf.getInt("s3copy.workerthreads", 10);
    // If use multipart or not.
    this.useMultipart = conf.getBoolean("s3copy.multipart", true);
    // Multipart chunk size.
    this.chunkSize = conf.getInt("s3copy.chunksizemb", 32) * 1024L * 1024;
    // Whether to verify checksum during transmit.
    this.verifyChecksum = conf.getBoolean("s3copy.checksum", true);
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
      .append(String.format("\tworker threads per task: %d\n", this.workerThreads));
    log.info(sb.toString());
  }

  public S3CopyOptions(String args[]) {
    Options options = new Options();

    SimpleOption helpOption = options.noArg("--help", "Print help text");
    SimpleOption verbose = options.noArg("--verbose", "be verbose");
    OptionWithArg srcOption = options.withArg("--src", "Source directory");
    OptionWithArg destOption = options.withArg("--dest", "Dest directory");

    options.parseArguments(args);
    if (helpOption.defined()) {
      log.info(options.helpText());
      this.helpDefined = true;
      return;
    }
    srcOption.require();
    if (verbose.defined()) {
      this.verbose = true;
    }
    if (srcOption.defined()) {
      srcPath = srcOption.getValue();
    }
    if (destOption.defined()) {
      destPath = destOption.getValue();
      if (destPath.endsWith("/")) {
        destPath = destPath.substring(0, destPath.length() - 1);
      }
    }
  }

}

