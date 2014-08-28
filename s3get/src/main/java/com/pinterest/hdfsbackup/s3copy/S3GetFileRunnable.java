package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.s3tools.S3Downloader;
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
  S3CopyOptions options;

  public S3GetFileRunnable(FilePairInfo filePair, S3CopyReducer reducer) {
    this.s3CopyReducer = reducer;
    this.filePair = filePair;
    this.options = reducer.options;
  }

  public S3GetFileRunnable(FilePairInfo filePair, S3CopyReducer reducer, S3CopyOptions options) {
    this.s3CopyReducer = reducer;
    this.filePair = filePair;
    this.options = options;
  }

  @Override
  public void run() {
    log.info("Runnable start processing file pair: " + this.filePair.toString());
    S3Downloader s3Downloader = new S3Downloader(this.s3CopyReducer.getConf(), this.options);
    String destFilename = this.filePair.destFile.toString();
    boolean ret = s3Downloader.DownloadFile(this.filePair.srcFile.toString(),
                              destFilename.equals("") ? null : destFilename,
                              options.verifyChecksum);
    log.info("finish file pair: " + this.filePair.toString() + ", res = " + ret);
    if (ret) {
      this.s3CopyReducer.removeUnfinishedFile(this.filePair);
    }
  }
}
