package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.utils.FilePair;
import com.pinterest.hdfsbackup.utils.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;

public class HDFSCopyFileRunnable implements Runnable {
  private static final Log log = LogFactory.getLog(S3PutFileRunnable.class);
  HDFSCopyMapper hdfsCopyMapper;
  FilePair filePair;
  S3CopyOptions options;


  public HDFSCopyFileRunnable(FilePair filePair, HDFSCopyMapper mapper, S3CopyOptions options) {
    this.hdfsCopyMapper = mapper;
    this.filePair = filePair;
    this.options = options;
  }

  @Override
  public void run() {
    log.info("Runnable start processing file pair: " + this.filePair.toString());
    String destFilename = this.filePair.destFile.toString();
    String srcFilename = this.filePair.srcFile.toString();
    boolean ret = false;

    InputStream inputStream;
    OutputStream outputStream;
    int attempt = 1;
    int maxRetry = 5;
    boolean willRetry = false;

    while (attempt <= maxRetry) {
      inputStream = FileUtils.openHDFSInputStream(srcFilename, this.hdfsCopyMapper.conf);
      outputStream = FileUtils.openHDFSOutputStream(destFilename, this.hdfsCopyMapper.conf);

      if (inputStream == null || outputStream == null) {
        log.info(String.format("attempt %d: failed to open input/output streams.", attempt));
        attempt++;
        willRetry = true;
      } else {
        MessageDigest md = null;
        long bytesCopied = FileUtils.copyStream(inputStream,
                                                outputStream,
                                                md,
                                                this.hdfsCopyMapper.reporter,
                                                this.hdfsCopyMapper.bwMonitor);
        if (bytesCopied < 0) {
          log.error(String.format("attempt %d: Failed to copy src: %s, tgt: %s",
              attempt, srcFilename, destFilename));
          attempt++;
          willRetry = true;
        } else {
          if (bytesCopied != this.filePair.fileSize.get()) {
            log.error(String.format("actual copied %d bytes != FS bytes %d: src: %s, tgt: %s",
                bytesCopied, this.filePair.fileSize.get(),
                srcFilename, destFilename));
          }
          willRetry = false;
          ret = true;
        }
      }

      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (outputStream != null) {
        try {
          outputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (willRetry) {
        FileUtils.deleteHDFSDir(destFilename, this.hdfsCopyMapper.conf);
      } else {
        break;
      }
    }

    log.info(String.format("finish file pair in %d attempts, ret = %s, file pair: %s",
                           attempt, ret ? "success" : "failure", this.filePair.toString()));
    if (ret) {
      this.hdfsCopyMapper.removeUnfinishedFile(this.filePair);
    }
  }
}

