package com.pinterest.hdfsbackup.s3tools;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.pinterest.hdfsbackup.utils.DirEntry;
import com.pinterest.hdfsbackup.utils.FileUtils;
import com.pinterest.hdfsbackup.utils.NetworkBandwidthMonitor;
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
import java.util.Map;
import java.util.TreeMap;
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
  NetworkBandwidthMonitor bwMonitor;
  // md5 checksum of the last downloaded file.
  String lastMD5Checksum = "";

  public S3Uploader(Configuration conf,
                    S3CopyOptions options,
                    Progressable progress) {
    this(conf, options, progress, null);
  }

  public S3Uploader(Configuration conf,
                    S3CopyOptions options,
                    Progressable progress,
                    NetworkBandwidthMonitor bwMonitor) {
    this.conf = conf;
    this.s3client = S3Utils.createAmazonS3Client(conf);
    //this.threadPool = Utils.createDefaultExecutorService();
    this.options = options;
    this.progress = progress;
    this.bwMonitor = bwMonitor;
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
   * Upload a HDFS file to S3.  It performs the following ops:
   * 1. Compute the HDFS file original checksum before uploading. This checksum
   *    is piggybacked to the S3 object.
   *    The alternative would be to change S3 object metadata to include the checksum
   *    after upload finishes.  However this requires a s3.copyObject() to alter object
   *    metadata, which is very slow at S3 side to move data, compared to HDFS data access.
   * 2. Create object metadata including the original checksum, and initiates multipart-upload
   *    to S3.
   *
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
    // 1. get S3 bucket and key of the destination file.
    Path destPath = new Path(destFilename);
    URI destUri = destPath.toUri();
    String bucket = destUri.getHost();
    String key = destUri.getPath();
    if (key.startsWith("/")) {
      key = key.substring(1);
    }

    // 2. We will compute the HDFS file original checksum before uploading. This checksum
    //    is piggybacked to the S3 object.
    int maxRetry = 5;
    int retry = 0;
    String srcDigest = null;
    boolean srcDigestSuccess = false;
    boolean computeSourceChecksum = true;
    if (computeSourceChecksum) {
      while (retry < maxRetry) {
        retry++;
        MessageDigest md = null;
        try {
          md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
          e.printStackTrace();
          return false;
        }
        if (FileUtils.computeHDFSDigest(srcFilename, this.conf, md, this.bwMonitor)) {
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
    }

    // 3. Run multipart upload to S3, with retries.
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(srcFileSize);
    if (srcDigest != null) {
      metadata.setContentMD5(srcDigest);
      metadata.addUserMetadata("contentmd5", srcDigest);
    }
    metadata.addUserMetadata("contentlength", String.valueOf(srcFileSize));
    retry = 0;
    log.info("Will multipart-upload " + srcFilename
                 + " with metadata:" + S3Utils.objectMetadataToString(metadata));
    while (retry < maxRetry) {
      retry++;
      if (multipartUploadFile(srcFilename, bucket, key, metadata)) {
        return true;
      }
    }
    log.info(String.format("multipart-upload failed: %s/%s", bucket, key));
    return false;
  }

  /**
   * Upload a HDFS file to S3, using multipart upload.
   *
   *
   * @param srcFilename
   * @param destBucket
   * @param destKey
   * @param metadata The object metadata to assign to this to-be created S3 object.
   *                 It should contain the correct md5 checksum.
   * @return
   */
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
      long bytesCopied = FileUtils.copyStream(inputStream,
                                                 s3OutStream,
                                                 md,
                                                 this.progress,
                                                 this.bwMonitor);
      if (bytesCopied != metadata.getContentLength()) {
        log.info(String.format("multipart-upload: %s/%s:  copied %d bytes != actual bytes %d",
                                  destBucket, destKey, bytesCopied, metadata.getContentLength()));
        return false;
      }
      // Flush all buffered data to S3.
      s3OutStream.close();
      s3OutStream = null;

      String currChecksum = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));

      // double check the online checksum with user-provided checksum.
      if (metadata.getUserMetadata().containsKey("contentmd5")) {
        if (currChecksum.equals(metadata.getUserMetadata().get("contentmd5"))) {
          log.info(String.format("multipart-upload success and checksum passed: %s/%s: " +
                                     "copied %d bytes",
                                    destBucket, destKey, bytesCopied));
          return true;
        } else {
          log.info(String.format("multipart-upload ok checksum mismatch: %s/%s: ",
                                    destBucket, destKey));
          return false;
        }
      }

      // Save the online-computed checksum to S3 if the metadata doesn't already have that.
      // We use s3.copyObject(bucket, key) to copy the obj to the same location
      // with metadata attached.
      // However, "copy object" seems triggers real data momvement at S3, and is much slower
      // than data move at HDFS.
      Map<String, String> userMetadata = new TreeMap<String, String>();
      userMetadata.put("contentmd5", currChecksum);
      log.info(String.format("Will piggyback md5checksum to %s/%s", destBucket, destKey));
      if (S3Utils.addS3ObjectUserMetadata(this.s3client, destBucket, destKey, userMetadata)) {
        log.info(String.format("multipart-upload success and later checksum passed: %s/%s: " +
                                   "copied %d bytes",
                                  destBucket, destKey, bytesCopied));
        return true;
      } else {
        log.info(String.format("multipart-upload ok but later checksum failed: %s/%s: ",
                                  destBucket, destKey));
        return false;
      }

    } catch (IOException e) {
      log.info(String.format("Exception when multipart upload: %s/%s", destBucket, destKey));
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