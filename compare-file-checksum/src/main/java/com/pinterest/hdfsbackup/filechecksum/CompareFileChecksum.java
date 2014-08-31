package com.pinterest.hdfsbackup.filechecksum;

import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.s3tools.S3Downloader;
import com.pinterest.hdfsbackup.utils.FSType;
import com.pinterest.hdfsbackup.utils.FileUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

import java.nio.charset.Charset;
import java.security.MessageDigest;

/**
 * Created by shawn on 8/30/14.
 */
public class CompareFileChecksum {
  private static final Log log = LogFactory.getLog(CompareFileChecksum.class);


  public static void main(String args[]) {
    CompareFileChecksumOptions options = new CompareFileChecksumOptions(args);
    if (options.helpDefined) {
      return;
    }

    Configuration conf = new Configuration();

    // We only support S3 and HDFS file system for now.
    FSType srcType = FileUtils.getFSType(options.srcPath);
    FSType destType = FileUtils.getFSType(options.destPath);
    if (srcType != FSType.S3 && srcType != FSType.HDFS &&
        destType != FSType.HDFS && destType != FSType.S3) {
      log.info("only HDFS and S3 are supported right now.");
      System.exit(1);
    }

    String srcChecksum = "";
    String destChecksum = "";
    S3CopyOptions s3CopyOptions = new S3CopyOptions();
    s3CopyOptions.populateFromConfiguration(conf);
    Progressable progress = new Progressable() {
      @Override
      public void progress() {}};
    int ret = 0;
    try {
      if (srcType == FSType.S3) {
        S3Downloader s3Downloader = new S3Downloader(conf, s3CopyOptions, progress);
        if (s3Downloader.DownloadFile(options.srcPath, null, false)) {
          srcChecksum = s3Downloader.getLastMD5Checksum();
        } else {
          log.info("failed to download src s3 file: " + options.srcPath);
          System.exit(1);
        }
      } else if (srcType == FSType.HDFS) {
        MessageDigest md = MessageDigest.getInstance("MD5");
        if (FileUtils.computeHDFSDigest(options.srcPath, conf, md)) {
          srcChecksum = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));
        } else {
          log.info("failed to compute hdfs digest: " + options.srcPath);
          System.exit(1);
        }
      }

      if (destType == FSType.S3) {
        S3Downloader s3Downloader = new S3Downloader(conf, s3CopyOptions, progress);
        if (s3Downloader.DownloadFile(options.destPath, null, false)) {
          destChecksum = s3Downloader.getLastMD5Checksum();
        } else {
          log.info("failed to download dest file: " + options.srcPath);
          System.exit(1);
        }
      } else if (destType == FSType.HDFS) {
        MessageDigest md = MessageDigest.getInstance("MD5");
        if (FileUtils.computeHDFSDigest(options.destPath, conf, md)) {
          srcChecksum = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));
        } else {
          log.info("failed to compute hdfs digest: " + options.srcPath);
          System.exit(1);
        }
      }

      if (srcChecksum.equals(destChecksum)) {
        log.info("Congratulations! The two files' checksums match!");
        ret = 0;
      } else {
        log.info(String.format("Unfortunately the two files' checksums mismatch: %s :: %s",
                                  srcChecksum, destChecksum));
        ret = 1;
      }
    } catch (Exception e) {
      ret = 1;
    }
    System.exit(ret);
  }
}
