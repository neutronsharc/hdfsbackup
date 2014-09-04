package com.pinterest.hdfsbackup.comparedir;

import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.s3tools.S3Downloader;
import com.pinterest.hdfsbackup.utils.FSType;
import com.pinterest.hdfsbackup.utils.FilePair;
import com.pinterest.hdfsbackup.utils.FileUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.charset.Charset;
import java.security.MessageDigest;

/**
 * Created by shawn on 9/3/14.
 */
public class CompareDirRunnable implements Runnable {
  private static final Log log = LogFactory.getLog(CompareDirRunnable.class);
  final CompareDirMapper compareDirMapper;
  final boolean isSourceFile;
  final FilePair filePair;
  final S3CopyOptions options;
  final String filename;  // This is the file in the file-pair to work on.



  public CompareDirRunnable(FilePair filePair,
                            boolean isSourceFile,
                            CompareDirMapper mapper,
                            S3CopyOptions options) {
    this.compareDirMapper = mapper;
    this.filePair = filePair;
    this.isSourceFile = isSourceFile;
    this.options = options;
    this.filename = isSourceFile ? filePair.srcFile.toString() : filePair.destFile.toString();
  }

  @Override
  public void run() {
    FSType fsType = FileUtils.getFSType(this.filename);
    if (fsType != FSType.S3 && fsType != FSType.HDFS) {
      log.info("FS type unsupported");
      return;
    }
    String checksum = null;
    int retry = 0;
    int maxRetry = 3;
    boolean success = false;
    while (retry < maxRetry) {
      retry++;
      if (fsType == FSType.S3) {
        S3Downloader s3Downloader = new S3Downloader(this.compareDirMapper.getConf(),
                                                     this.options,
                                                     this.compareDirMapper.reporter);
        if (s3Downloader.DownloadFile(this.filename, null, false)) {
          checksum = new String(s3Downloader.getLastMD5Checksum());
          success = true;
          break;
        } else {
          log.info("failed to compute s3 checksum: " + this.filename);
        }
      } else if (fsType == FSType.HDFS) {
        MessageDigest md = null;
        try {
          md = MessageDigest.getInstance("MD5");
          if (FileUtils.computeHDFSDigest(this.filename, this.compareDirMapper.getConf(), md)) {
            checksum = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));
            success = true;
            break;
          } else {
            log.info("failed to compute hdfs digest: " + this.filename);
          }
        } catch (Exception e) {}
      }
    }
    if (!success) {
      log.info("failed to compute checksum for filepair " +
                   (this.isSourceFile ? "source: " : "dest: ") +
                   this.filePair.toString());
    } else if (!this.compareDirMapper.setFilePairChecksum(this.filePair,
                                                          checksum,
                                                          this.isSourceFile)) {
      log.info("failed to set checksum for filepair " +
                   (this.isSourceFile ? "source: " : "dest: ") +
                   this.filePair.toString());
    } else {
    }
  }
}
