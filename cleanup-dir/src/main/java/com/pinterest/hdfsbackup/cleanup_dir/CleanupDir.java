package com.pinterest.hdfsbackup.cleanup_dir;

import com.pinterest.hdfsbackup.utils.DirWalker;
import junit.framework.Test;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;

/**
 * Created by shawn on 10/9/14.
 */
public class CleanupDir {
  private static final Log log = LogFactory.getLog(Test.class);

  public static void main(String args[]) {
    log.info("run with options: " + Arrays.toString(args));
    Configuration conf = new Configuration();

    CleanupDirOptions options = new CleanupDirOptions(args);
    if (options.helpDefined) {
      return;
    }

    DirWalker dirWalker = new DirWalker(conf);

    // Trim all files in the given dir older than the given lower bound time.
    dirWalker.deleteFiles(options.srcPath, options.lowerboundDatetimeString, options.dryrun);
  }
}
