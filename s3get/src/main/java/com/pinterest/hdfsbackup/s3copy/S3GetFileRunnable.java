package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.s3tools.S3Downloader;
import com.pinterest.hdfsbackup.utils.FilePair;
import com.pinterest.hdfsbackup.utils.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by shawn on 8/26/14.
 */
public class S3GetFileRunnable implements Runnable {
  private static final Log log = LogFactory.getLog(S3GetFileRunnable.class);
  S3GetReducer s3GetReducer;
  FilePair filePair;
  S3CopyOptions options;

  public S3GetFileRunnable(FilePair filePair, S3GetReducer reducer) {
    this.s3GetReducer = reducer;
    this.filePair = filePair;
    this.options = reducer.options;
  }

  public S3GetFileRunnable(FilePair filePair, S3GetReducer reducer, S3CopyOptions options) {
    this.s3GetReducer = reducer;
    this.filePair = filePair;
    this.options = options;
  }

  @Override
  public void run() {
    log.info("Runnable start processing file pair: " + this.filePair.toString());
    S3Downloader s3Downloader = new S3Downloader(this.s3GetReducer.getConf(),
                                                 this.options,
                                                 this.s3GetReducer.reporter);
    String destFilename = this.filePair.destFile.toString();
    String srcFilename = this.filePair.srcFile.toString();
    boolean ret = false;
    if (srcFilename.endsWith("/")) {
      // src entry is an empty dir,  only needs to create a dest dir.
      if (!destFilename.equals("")) {
        ret = FileUtils.createHDFSDir(destFilename, this.s3GetReducer.getConf());
      } else {
        ret = true;
      }
    } else {
      ret = s3Downloader.DownloadFile(this.filePair.srcFile.toString(),
                                         destFilename.equals("") ? null : destFilename,
                                         options.verifyChecksum);
    }
    log.info("finish file pair: " + this.filePair.toString() + ", res = " + ret);
    if (ret) {
      this.s3GetReducer.removeUnfinishedFile(this.filePair);
    }
  }
}
