package com.pinterest.hdfsbackup.filechecksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.pinterest.hdfsbackup.options.OptionWithArg;
import com.pinterest.hdfsbackup.options.Options;
import com.pinterest.hdfsbackup.options.SimpleOption;

/**
 * Created by shawn on 8/30/14.
 */
public class CompareFileOptions {
  private static final Log log = LogFactory.getLog(CompareFileOptions.class);
  // Below options are provided at cmd line through "--opt=value"
  public String srcPath = null;
  public String destPath = null;
  public boolean helpDefined = false;
  public boolean verbose = false;

  public void showOptions() {
    StringBuilder sb = new StringBuilder();
    sb.append("Compare file options: \n")
        .append("\tsource: " + this.srcPath + "\n")
        .append("\ttarget: " + this.destPath + "\n");
    log.info(sb.toString());
  }

  public CompareFileOptions(String args[]) {
    Options options = new Options();

    SimpleOption helpOption = options.noArg("--help", "Print help text");
    SimpleOption verbose = options.noArg("--verbose", "be verbose");
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
  }
}
