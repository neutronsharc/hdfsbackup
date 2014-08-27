package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.utils.FilePairInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by shawn on 8/26/14.
 */
public class S3GetFileRunnable implements Runnable {
  private static final Log log = LogFactory.getLog(S3GetFileRunnable.class);
  S3CopyReducer s3CopyReducer;
  FilePairInfo filePair;

  S3GetFileRunnable(FilePairInfo filePair, S3CopyReducer reducer) {
    this.s3CopyReducer = reducer;
    this.filePair = filePair;
  }
  @Override
  public void run() {
    log.info("start processing file pair: " + this.filePair.toString());


  }
}
