package com.pinterest.hdfsbackup.cleanup_dir;

import com.pinterest.hdfsbackup.options.OptionWithArg;
import com.pinterest.hdfsbackup.options.Options;
import com.pinterest.hdfsbackup.options.SimpleOption;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sometimes HBase backup's hlog is produced too heavily, and a hlog backup dir contains
 * too many HLog files such that,  "hdfs dfs -ls [dir]" hits OutOfMemory error.
 * Consequently python tool to clean up hlogs stop working.
 *
 * This Java tool comes in handy to clean up old hlog files from a HDFS dir, regardless how big
 * a dir is.
 *
 * Created by shawn on 10/9/14.
 */
public class CleanupDirOptions {
  private static final Log log = LogFactory.getLog(CleanupDirOptions.class);

  public String srcPath = null;
  public boolean helpDefined = false;
  public boolean dryrun = false;
  public boolean verbose = false;
  public String lowerboundDatetimeString = null;

  public CleanupDirOptions(String args[]) {
    Options options = new Options();

    SimpleOption dryrunOption = options.noArg("--dryrun", "Do a dry run.");
    SimpleOption helpOption = options.noArg("--help", "Print help text");
    SimpleOption verbose = options.noArg("--verbose", "be verbose");
    OptionWithArg srcOption = options.withArg("--src", "Source directory");
    OptionWithArg lowerboundTimeOption = options.withArg("--lowerBoundTime",
                                                            "lower bound date time string in the " +
                                                                "format of " +
                                                                "yyyy-mm-dd-hh-mm-ss-SSS");
    options.parseArguments(args);

    if (helpOption.defined()) {
      log.info(options.helpText());
      this.helpDefined = true;
      return;
    }
    srcOption.require();
    lowerboundTimeOption.require();
    if (verbose.defined()) {
      this.verbose = true;
    }
    if (srcOption.defined()) {
      this.srcPath = srcOption.getValue();
    }
    if (dryrunOption.defined()) {
      this.dryrun = true;
    }
    this.lowerboundDatetimeString = lowerboundTimeOption.getValue();
  }

}
