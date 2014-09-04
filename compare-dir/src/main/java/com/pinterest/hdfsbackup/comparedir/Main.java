package com.pinterest.hdfsbackup.comparedir;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * Created by shawn on 9/3/14.
 */
public class Main {
  private static final Log log = LogFactory.getLog(Main.class);

  public static void main(String args[]) {
    log.info("run with options: " + Arrays.toString(args));

    try {
      System.exit(ToolRunner.run(new CompareDir(), args));
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
