package com.pinterest.hdfsbackup.test;

import com.pinterest.hdfsbackup.options.OptionWithArg;
import com.pinterest.hdfsbackup.options.SimpleOption;
import com.pinterest.hdfsbackup.options.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Created by shawn on 8/23/14.
 */
public class TestOptions {
  private static final Log log = LogFactory.getLog(TestOptions.class);

  public String srcPath;
  public String destPath;
  public boolean helpDefined = false;

  public TestOptions() { }

  public TestOptions(String args[]) {
    Options options = new Options();

    SimpleOption helpOption = options.noArg("--help", "Print help text");
    OptionWithArg srcOption = options.withArg("--src", "Source directory");
    OptionWithArg destOption = options.withArg("--dest", "Dest directory");

    options.parseArguments(args);

    if (helpOption.defined()) {
      log.info(options.helpText());
      helpDefined = true;
    }

    //srcOption.require();
    //destOption.require();

    if (srcOption.defined()) {
      srcPath = srcOption.getValue();
    }

    if (destOption.defined()) {
      destPath = destOption.getValue();
    }

  }

}
