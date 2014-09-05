package com.pinterest.hdfsbackup.comparefile;

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
public class CompareFile {
  private static final Log log = LogFactory.getLog(CompareFile.class);


  public static void main(String args[]) {
    /*CompareFileOptions options = new CompareFileOptions(args);
    if (options.helpDefined) {
      return;
    }*/

    Configuration conf = new Configuration();
    String srcChecksum = "";
    String destChecksum = "";
    S3CopyOptions s3CopyOptions = new S3CopyOptions(args);
    s3CopyOptions.populateFromConfiguration(conf);
    s3CopyOptions.showCopyOptions();
    Progressable progress = new Progressable() {
      @Override
      public void progress() {}};

    FSType srcType = FileUtils.getFSType(s3CopyOptions.srcPath);

    // A special case: only read S3 and verify checksum.
    if (srcType == FSType.S3 && s3CopyOptions.destPath == null) {
      S3Downloader s3Downloader = new S3Downloader(conf, s3CopyOptions, progress);
      if (s3Downloader.DownloadFile(s3CopyOptions.srcPath, null, true)) {
        log.info("S3 file checksum verify success");
        System.exit(0);
      } else {
        log.info("S3 file checksum failed");
        System.exit(1);
      }
    }
    // Now, both src and dest are provided. We will compare the two files.
    // As of now only S3 and HDFS are supported.
    // TODO (shawn@):  support local disk files.
    FSType destType = FileUtils.getFSType(s3CopyOptions.destPath);
    if (srcType != FSType.S3 && srcType != FSType.HDFS &&
        destType != FSType.HDFS && destType != FSType.S3) {
      log.info("only HDFS and S3 are supported right now.");
      System.exit(1);
    }


    int ret = 0;

    try {
      log.info("First, read source file: " + s3CopyOptions.srcPath);
      if (srcType == FSType.S3) {
        S3Downloader s3Downloader = new S3Downloader(conf, s3CopyOptions, progress);
        if (s3Downloader.DownloadFile(s3CopyOptions.srcPath, null, false)) {
          srcChecksum = s3Downloader.getLastMD5Checksum();
        } else {
          log.info("failed to download src s3 file: " + s3CopyOptions.srcPath);
          System.exit(1);
        }
      } else if (srcType == FSType.HDFS) {
        MessageDigest md = MessageDigest.getInstance("MD5");
        if (FileUtils.computeHDFSDigest(s3CopyOptions.srcPath, conf, md)) {
          srcChecksum = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));
        } else {
          log.info("failed to compute hdfs digest: " + s3CopyOptions.srcPath);
          System.exit(1);
        }
      }

      log.info("Second, read dest file: " + s3CopyOptions.destPath);
      if (destType == FSType.S3) {
        S3Downloader s3Downloader = new S3Downloader(conf, s3CopyOptions, progress);
        if (s3Downloader.DownloadFile(s3CopyOptions.destPath, null, false)) {
          destChecksum = s3Downloader.getLastMD5Checksum();
        } else {
          log.info("failed to download dest file: " + s3CopyOptions.srcPath);
          System.exit(1);
        }
      } else if (destType == FSType.HDFS) {
        MessageDigest md = MessageDigest.getInstance("MD5");
        if (FileUtils.computeHDFSDigest(s3CopyOptions.destPath, conf, md)) {
          destChecksum = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));
        } else {
          log.info("failed to compute hdfs digest: " + s3CopyOptions.srcPath);
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
      log.info("Error when reading data!");
      ret = 1;
    }
    System.exit(ret);
  }
}
