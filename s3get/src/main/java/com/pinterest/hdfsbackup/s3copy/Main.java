package com.pinterest.hdfsbackup.s3copy;

import junit.framework.Test;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * Created by shawn on 8/26/14.
 */
public class Main {
  private static final Log log = LogFactory.getLog(Test.class);

  public static void main(String args[]) {
    log.info("run with options: " + Arrays.toString(args));
    try {
      System.exit(ToolRunner.run(new S3Copy(), args));
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
