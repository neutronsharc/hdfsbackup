package com.pinterest.hdfsbackup.comparedir;

import com.pinterest.hdfsbackup.options.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by shawn on 9/2/14.
 */
public class CompareDirOptions {
  private static final Log log = LogFactory.getLog(CompareDirOptions.class);
  // Below options are provided at cmd line through "--opt=value"
  public String srcPath = null;
  public String destPath = null;
  public boolean helpDefined = false;
  public boolean verbose = false;
  public boolean compareChecksum = false;

  public void showOptions() {
    StringBuilder sb = new StringBuilder();
    sb.append("Compare file options: \n")
        .append("\tsource: " + this.srcPath + "\n")
        .append("\ttarget: " + this.destPath + "\n");
    log.info(sb.toString());
  }

  public CompareDirOptions(String args[]) {
    Options options = new Options();

    SimpleOption helpOption = options.noArg("--help", "Print help text");
    SimpleOption verbose = options.noArg("--verbose", "be verbose");
    SimpleOption compareChecksumOption = options.noArg("--checksum", "compare file checksums.");
    OptionWithArg srcOption = options.withArg("--src", "Source file");
    OptionWithArg destOption = options.withArg("--dest", "Destination file");

    options.parseArguments(args);
    if (helpOption.defined()) {
      log.info(options.helpText());
      this.helpDefined = true;
      return;
    }
    srcOption.require();
    destOption.require();
    if (verbose.defined()) {
      this.verbose = true;
    }
    if (srcOption.defined()) {
      srcPath = srcOption.getValue();
    }
    if (destOption.defined()) {
      destPath = destOption.getValue();
    }
    if (compareChecksumOption.defined()) {
      this.compareChecksum = true;
    }
  }
}
