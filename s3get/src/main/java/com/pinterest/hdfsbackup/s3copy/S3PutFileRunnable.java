package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.s3tools.S3Uploader;
import com.pinterest.hdfsbackup.utils.FilePair;
import com.pinterest.hdfsbackup.utils.S3Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by shawn on 9/2/14.
 */

public class S3PutFileRunnable implements Runnable {
  private static final Log log = LogFactory.getLog(S3PutFileRunnable.class);
  S3PutMapper s3PutMapper;
  FilePair filePair;
  S3CopyOptions options;


  public S3PutFileRunnable(FilePair filePair, S3PutMapper mapper, S3CopyOptions options) {
    this.s3PutMapper = mapper;
    this.filePair = filePair;
    this.options = options;
  }

  @Override
  public void run() {
    log.info("Runnable start processing file pair: " + this.filePair.toString());
    S3Uploader s3Uploader = new S3Uploader(this.s3PutMapper.getConf(),
                                           this.options,
                                           this.s3PutMapper.reporter);
    String destFilename = this.filePair.destFile.toString();
    String srcFilename = this.filePair.srcFile.toString();
    boolean ret = false;
    if (srcFilename.endsWith("/")) {
      // src entry is an empty dir,  only needs to create a dest dir.
      if (destFilename.equals("")) {
        ret = true;
      } else {
        // Create a S3 empty object with "/" at the end of name to emulate an empty dir.
        // For now, reuse the hadoop FS API to create an S3 object.
        log.info("will create S3 object: " + destFilename);
        if (S3Utils.createS3Directory(destFilename, this.s3PutMapper.conf)) {
          ret = true;
        } else {
          log.info("failed to create dest dir, filepair = " + this.filePair.toString());
          ret = false;
        }
      }
    } else {
      ret = s3Uploader.uploadFile(this.filePair.srcFile.toString(), destFilename);
    }
    log.info("finish file pair: " + this.filePair.toString() + ", res = " + ret);
    if (ret) {
      this.s3PutMapper.removeUnfinishedFile(this.filePair);
    }
  }
}
