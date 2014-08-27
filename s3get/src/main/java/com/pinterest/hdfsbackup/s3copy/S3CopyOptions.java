package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.options.OptionWithArg;
import com.pinterest.hdfsbackup.options.Options;
import com.pinterest.hdfsbackup.options.SimpleOption;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by shawn on 8/26/14.
 */
public class S3CopyOptions {
  private static final Log log = LogFactory.getLog(S3CopyOptions.class);

  public String srcPath = null;
  public String destPath = null;
  public boolean helpDefined = false;
  public boolean verifyChecksum = false;
  public boolean verbose = false;
  public boolean useMultipart = true;
  public int queueSize = 100;
  public int workerThreads = 10;
  public int chunkSizeMB = 32;

  public S3CopyOptions() { }

  public S3CopyOptions(String args[]) {
    Options options = new Options();

    SimpleOption helpOption = options.noArg("--help", "Print help text");
    SimpleOption verifyChecksum = options.noArg("--checksum", "Verify file checksum");
    SimpleOption verbose = options.noArg("--verbose", "be verbose");
    OptionWithArg srcOption = options.withArg("--src", "Source directory");
    OptionWithArg destOption = options.withArg("--dest", "Dest directory");
    OptionWithArg queueSize = options.withArg("--queuesize", "file list queue size");
    OptionWithArg numberThreads = options.withArg("--workerthreads",
                                                     "worker threads per task");
    OptionWithArg chunkSizeMB = options.withArg("--chunksizemb",
                                                   "multi-part chunk size in MB");
    SimpleOption useMultipart = options.noArg("--multipart", "use multipart operation");
    SimpleOption noMultipart = options.noArg("--nomultipart", "don't use multipart operation");

    options.parseArguments(args);
    srcOption.require();
    if (helpOption.defined()) {
      log.info(options.helpText());
      this.helpDefined = true;
      return;
    }
    if (useMultipart.defined() && noMultipart.defined()) {
      throw new RuntimeException("cannot define multipart and nomultipart at same time.");
    }
    if (useMultipart.defined()) {
      this.useMultipart = true;
    }
    if (noMultipart.defined()) {
      this.useMultipart = false;
    }
    if (queueSize.defined()) {
      this.queueSize = Integer.parseInt(queueSize.getValue());
    }
    if (numberThreads.defined()) {
      this.workerThreads = Integer.parseInt(numberThreads.getValue());
    }
    if (chunkSizeMB.defined()) {
      this.chunkSizeMB = Integer.parseInt(chunkSizeMB.getValue());
    }

    if (verifyChecksum.defined()) {
      this.verifyChecksum = true;
    }
    if (verbose.defined()) {
      this.verbose = true;
    }
    if (srcOption.defined()) {
      srcPath = srcOption.getValue();
    }
    if (destOption.defined()) {
      destPath = destOption.getValue();
    }
  }

}

