package com.pinterest.hdfsbackup.s3get;

import com.pinterest.hdfsbackup.options.OptionWithArg;
import com.pinterest.hdfsbackup.options.Options;
import com.pinterest.hdfsbackup.options.SimpleOption;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by shawn on 8/26/14.
 */
public class S3GetOptions {
  private static final Log log = LogFactory.getLog(S3GetOptions.class);

  public String srcPath = null;
  public String destPath = null;
  public boolean helpDefined = false;
  public boolean verifyChecksum = false;
  public boolean verbose = false;

  public S3GetOptions() { }

  public S3GetOptions(String args[]) {
    Options options = new Options();

    SimpleOption helpOption = options.noArg("--help", "Print help text");
    SimpleOption verifyChecksum = options.noArg("--checksum", "Verify file checksum");
    SimpleOption verbose = options.noArg("--verbose", "be verbose");
    OptionWithArg srcOption = options.withArg("--src", "Source directory");
    OptionWithArg destOption = options.withArg("--dest", "Dest directory");

    options.parseArguments(args);

    if (helpOption.defined()) {
      log.info(options.helpText());
      helpDefined = true;
      return;
    }
    if (verifyChecksum.defined()) {
      this.verifyChecksum = true;
    }
    if (verbose.defined()) {
      this.verbose = true;
    }
    srcOption.require();
    //destOption.require();
    if (srcOption.defined()) {
      srcPath = srcOption.getValue();
    }
    if (destOption.defined()) {
      destPath = destOption.getValue();
    }

  }

}

