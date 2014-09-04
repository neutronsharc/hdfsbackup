package com.pinterest.hdfsbackup.s3tools;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.pinterest.hdfsbackup.utils.DirEntry;
import com.pinterest.hdfsbackup.utils.FileUtils;
import com.pinterest.hdfsbackup.utils.S3Utils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by shawn on 9/1/14.
 */
public class S3Uploader {
  static final Log log = LogFactory.getLog(S3Uploader.class);
  Configuration conf;
  AmazonS3Client s3client;
  ThreadPoolExecutor threadPool;
  S3CopyOptions options;
  Progressable progress;
  // md5 checksum of the last downloaded file.
  String lastMD5Checksum = "";


  public S3Uploader(Configuration conf, S3CopyOptions options, Progressable progress) {
    this.conf = conf;
    this.s3client = S3Utils.createAmazonS3Client(conf);
    //this.threadPool = Utils.createDefaultExecutorService();
    this.options = options;
    this.progress = progress;
  }

  public void close() {

    this.s3client.shutdown();
  }

  public String getLastMD5Checksum() {
    return this.lastMD5Checksum;
  }


  /**
   * Upload HDFS file to S3.
   * @param srcEntry
   * @param destDirname
   * @return
   */
  public boolean uploadFile(DirEntry srcEntry, String destDirname) {
    String srcFilename = srcEntry.baseDirname + "/" + srcEntry.entryName;
    String destFilename = null;
    if (destDirname != null) {
      if (destDirname.endsWith("/")) {
        destFilename = destDirname + srcEntry.entryName;
      } else {
        destFilename = destDirname + "/" + srcEntry.entryName;
      }
    }
    return uploadFile(srcFilename, destFilename);
  }


  /**
   * Upload a HDFS file to S3.
   * @param srcFilename
   * @param destFilename
   * @return
   */
  public boolean uploadFile(String srcFilename, String destFilename) {
    if (this.s3client == null) {
      log.info("Error: S3Client not initialized");
      return false;
    }
    long srcFileSize = FileUtils.getHDFSFileSize(srcFilename, this.conf);
    if (srcFileSize < 0) {
      log.info("Failed to get srcfile size: " + srcFilename);
      return false;
    }
    // get S3 bucket and key of the destination file.
    Path destPath = new Path(destFilename);
    URI destUri = destPath.toUri();
    String bucket = destUri.getHost();
    String key = destUri.getPath();
    if (key.startsWith("/")) {
      key = key.substring(1);
    }
    int maxRetry = 5;
    int retry = 0;
    String srcDigest = null;
    boolean srcDigestSuccess = false;
    // Compute the source file checksum.
    while (retry < maxRetry) {
      retry++;
      MessageDigest md = null;
      try {
        md = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
        return false;
      }
      if (FileUtils.computeHDFSDigest(srcFilename, this.conf, md)) {
        srcDigest = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));
        srcDigestSuccess = true;
        break;
      } else {
        log.info("failed src file checksum attempt " + retry + ": " + srcFilename);
      }
    }
    if (!srcDigestSuccess) {
      log.info("Unable to get source file checksum: " + srcFilename);
      return false;
    }

    // Prepare metadata.
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(srcFileSize);
    metadata.setContentMD5(srcDigest);
    metadata.addUserMetadata("contentmd5", srcDigest);
    metadata.addUserMetadata("contentlength", String.valueOf(srcFileSize));

    retry = 0;
    log.info("Will multipart-upload " + srcFilename
                 + " with metadata:" + S3Utils.objectMetadataToString(metadata));
    // Upload to S3, with retries.
    while (retry < maxRetry) {
      retry++;
      if (multipartUploadFile(srcFilename, bucket, key, metadata)) {
        return true;
      }
    }
    log.info(String.format("multipart-upload failed: %s/%s", bucket, key));
    return false;
  }

  private boolean multipartUploadFile(String srcFilename,
                                      String destBucket,
                                      String destKey,
                                      ObjectMetadata metadata) {
    OutputStream s3OutStream = null;
    InputStream inputStream = null;
    String tempDirname = "/tmp/" + UUID.randomUUID();
    FileUtils.createLocalDir(tempDirname);
    log.info(String.format("will multipart-upload %s to %s/%s: chunksize = %d, "
                               + "expects %d bytes, use temp dir: %s",
                              srcFilename, destBucket, destKey,
                              this.options.chunkSize,
                              metadata.getContentLength(),
                              tempDirname));
    try {
      inputStream = FileUtils.openHDFSInputStream(srcFilename, this.conf);
      s3OutStream = new MultipartUploadOutputStream(this.s3client,
                                                       destBucket,
                                                       destKey,
                                                       metadata,
                                                       this.options,
                                                       this.conf,
                                                       this.progress,
                                                       tempDirname);
      if (inputStream == null || s3OutStream == null) {
        log.info("multipart-upload: failed to open input/output streams.");
        return false;
      }
      MessageDigest md;
      try {
        md = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        log.info("multipart-upload: fail to get digest instance");
        return false;
      }
      long bytesCopied = FileUtils.copyStream(inputStream, s3OutStream, md);
      if (bytesCopied != metadata.getContentLength()) {
        log.info(String.format("multipart-upload: %s/%s:  copied %d bytes != actual bytes %d",
                                  destBucket, destKey, bytesCopied, metadata.getContentLength()));
        return false;
      }
      // Flush all buffered data to S3.
      s3OutStream.close();
      s3OutStream = null;
      // double check the online checksum with user-provided checksum.
      String currChecksum = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));
      if (currChecksum.equals(metadata.getUserMetadata().get("contentmd5"))) {
        log.info(String.format("multipart-upload success and checksum passed: %s/%s: " +
                                   "copied %d bytes",
                                  destBucket, destKey, bytesCopied));
      } else {
        log.info(String.format("multipart-upload checksum mismatch: %s/%s: ", destBucket, destKey));
        return false;
      }
      // TODO: save the online-computed checksum to S3 as the object's metadata.
      return true;
    } catch (IOException e) {
      log.info(String.format("Error when multipart upload"));
      e.printStackTrace();
      return false;
    } finally {
      try {
        if (inputStream != null) {
          inputStream.close();
        }
        if (s3OutStream != null) {
          s3OutStream.close();
        }
      } catch (IOException e) {}
      FileUtils.deleteLocalDir(tempDirname);
    }

  }
}