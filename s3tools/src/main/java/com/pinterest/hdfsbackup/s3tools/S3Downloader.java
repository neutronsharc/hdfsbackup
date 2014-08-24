package com.pinterest.hdfsbackup.s3tools;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.pinterest.hdfsbackup.utils.FileUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

/**
 * Created by shawn on 8/22/14.
 */
public class S3Downloader {
  static final Log log = LogFactory.getLog(S3Downloader.class);
  Configuration conf;
  AmazonS3Client s3client;

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public S3Downloader(Configuration conf) {
    this.conf = conf;
    this.s3client = S3Utils.createAmazonS3Client(conf);
  }

  public boolean DownloadFile(String bucket,
                              String key,
                              String destFilename,
                              boolean doChecksum) {
    if (this.s3client != null) {
      return DownloadFile(this.s3client, bucket, key, destFilename, doChecksum);
    } else {
      log.info("Error: S3Client not initialized");
      return false;
    }
  }

  /**
   * Download a S3 object "bucket/key" to the destFilename.
   * @param s3client
   * @param bucket
   * @param key
   * @param destFilename
   * @return
   */
  public boolean DownloadFile(AmazonS3Client s3client,
                                     String bucket,
                                     String key,
                                     String destFilename,
                                     boolean doChecksum) {
    ObjectMetadata metadata = S3Utils.getObjectMetadata(s3client, bucket, key);
    if (metadata == null) {
      log.error("fail to get object metadat : " + bucket + "/" + key);
      return false;
    }
    // TODO: check if destfile parent dir exsits.
    return DownloadFile(s3client, bucket, key, metadata, destFilename, doChecksum);
  }

  public boolean DownloadFile(AmazonS3Client s3client,
                                     String bucket,
                                     String key,
                                     ObjectMetadata metadata,
                                     String destFilename,
                                     boolean doChecksum) {
    log.info(String.format("will download S3 obj %s/%s to: %s,  metadata = %s",
                              bucket, key, destFilename, S3Utils.objectMetadataToString(metadata)));
    Path destFilePath = null;
    FileSystem destFs = null;
    try {
      // Create the destination parent directory first.
      destFilePath = new Path(destFilename);
      destFs = destFilePath.getFileSystem(this.conf);
      destFs.mkdirs(destFilePath.getParent());
    } catch (Exception e) {
      log.info("failed to get filesystem for: " + destFilename);
      return false;
    }
    int maxRetry = 3;
    int retry = 0;

    // TODO: check if the object size too big.
    if (metadata.getContentLength() == 0) {
      while (retry < maxRetry) {
        retry++;
        try {
          FSDataOutputStream out = destFs.create(destFilePath);
          out.close();
          log.info("created zero-file: " + destFilename);
          return true;
        } catch (IOException e) {
          log.info("attempt " + retry + ": failed to create zero-file: " + destFilename);
        }
      }
      return false;
    } else if (metadata.getContentLength() >= 1024L * 1024 * 1024) {
        // user range-get multi-part download.
        return true;
    } else {
      // Download the file as a whole single object.
      while (retry < maxRetry) {
        retry++;
        if (DownloadAsOneObject(s3client, bucket, key, metadata, destFilename, doChecksum)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean DownloadAsOneObject(AmazonS3Client s3client,
                                     String bucket,
                                     String key,
                                     ObjectMetadata metadata,
                                     String destFilename,
                                     boolean doChecksum) {
    S3Object s3Object = null;
    boolean result = false;
    int maxRetry = 1;
    int retry = 0;
    // Step 1:  download the s3 object
    while (retry < maxRetry) {
      retry++;
      try {
        s3Object = s3client.getObject(bucket, key);
        result = true;
        break;
      } catch (AmazonServiceException ase) {
        log.info("ServiceException: " + S3Utils.AWSServiceExceptionToString(ase));
        //log.info("ServiceException: ", ase);
      } catch (AmazonClientException ace) {
        log.info("ClientException: " + ace.getMessage());
      }
    }
    if (!result) {
      log.info(String.format("Error: failed to download S3 file %s/%s, " +
                                 "metadata = %s",
                                bucket, key,
                                S3Utils.objectMetadataToString(metadata)));
      return false;
    }
    // Step 2:  copy the S3 object to buffer, and verify checksum
    Map<String, String> userMetadata = metadata.getUserMetadata();
    boolean hasChecksum = true;
    String expectedDigest = "";
    String actualDigest = "";
    long bytesCopied = 0;

    if (userMetadata.containsKey("ContentMD5")) {
      expectedDigest = userMetadata.get("ContentMD5");
      log.info(String.format("S3 obj %s/%s user-provide md5 = %s", bucket, key, expectedDigest));
    } else if (metadata.getContentMD5() != null) {
      expectedDigest = metadata.getContentMD5();
      log.info(String.format("S3 obj %s/%s system md5 = %s", bucket, key, expectedDigest));
    } else {
      hasChecksum = false;
      log.info(String.format("S3 obj %s/%s has no MD5 checksum", bucket, key));
    }
    ByteArrayOutputStream bOutput = null;
    DigestInputStream s3DigestInput = null;
    byte[] buffer = new byte[1024 * 1024];
    bytesCopied = 0;
    try {
      bOutput = new ByteArrayOutputStream((int)(metadata.getContentLength()));
      s3DigestInput = new DigestInputStream(s3Object.getObjectContent(),
                                               MessageDigest.getInstance("MD5"));
      int len;
      while ((len = s3DigestInput.read(buffer)) > 0) {
        bOutput.write(buffer, 0, len);
        bytesCopied += len;
      }
    } catch (IOException e) {
      log.info("read object input stream failed.");
      return false;
    } catch (NoSuchAlgorithmException e) {
      log.info("no such digest algorithm.");
      return false;
    } finally {
      try {
        if (bOutput != null) bOutput.close();
        if (s3DigestInput != null) s3DigestInput.close();
        log.info("close digest input...");
      } catch (Exception excp) {
      }
    }
    // Make sure we get expected number of bytes.
    if (bytesCopied != metadata.getContentLength() ||
        bOutput.size() != (int)(metadata.getContentLength())) {
      log.info(String.format("download S3 obj %s/%s size %d != expected_size %d",
                                bucket, key, bytesCopied, metadata.getContentLength()));
      return false;
    }

    if (hasChecksum && doChecksum) {
      actualDigest = new String(Base64.encodeBase64(s3DigestInput.getMessageDigest().digest()),
                                   Charset.forName("UTF-8"));
      if (expectedDigest.equals(actualDigest)) {
        log.info(String.format("download s3obj %s/%s checksum matched: %s",
                                  bucket, key, expectedDigest));
      } else {
        log.info(String.format("download s3obj %s/%s checksum mismatch: %s : %s",
                                  bucket, key, expectedDigest, actualDigest));
        return false;
      }
    }

    // Step 3: copy from buffer to destination file.
    retry = 0;
    ByteArrayInputStream bInput = null;
    OutputStream fileOutStream = null;
    while (retry < maxRetry) {
      retry++;
      try {
        bytesCopied = 0;
        if (bInput == null) {
          bInput = new ByteArrayInputStream(bOutput.toByteArray());
        }
        if (fileOutStream == null) {
          fileOutStream = FileUtils.openOutputStream(destFilename);
        }
        if (fileOutStream == null || bInput == null) {
          continue;
        }
        int len = 0;
        while ((len = bInput.read(buffer)) > 0) {
          fileOutStream.write(buffer, 0, len);
          bytesCopied += len;
        }
        if (bytesCopied == metadata.getContentLength()) {
          log.info(String.format("download  %s/%s size %d: save to dest %s with success",
                           bucket, key, bytesCopied, destFilename));
          return true;
        } else {
          log.info(String.format("Error: download %s/%s size %d: " +
                                    "only saved %d bytes to dest %s ",
                           bucket, key, metadata.getContentLength(), bytesCopied, destFilename));
          return false;
        }
      } catch (Exception e) {
        log.info(String.format("download s3obj attempt %d: %s/%s: error copy byte %d",
                                  retry, bucket, key, bytesCopied));
      } finally {
        try {
          log.info("close output stream: " + destFilename + ", copied bytes = " + bytesCopied);
          if (bInput != null) bInput.close();
          if (fileOutStream != null) fileOutStream.close();
        } catch (IOException excp) {}
      }
    }
    return false;
  }

  public boolean DownloadFileRange(AmazonS3Client s3Client,
                                          String bucket,
                                          String key) {
    return true;
  }

}