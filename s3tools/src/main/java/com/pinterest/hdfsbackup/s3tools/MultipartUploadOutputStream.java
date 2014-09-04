package com.pinterest.hdfsbackup.s3tools;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.pinterest.hdfsbackup.utils.FileUtils;
import com.pinterest.hdfsbackup.utils.ProgressableResettableBufferedFileInputStream;
import com.pinterest.hdfsbackup.utils.Utils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.nio.charset.Charset;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class represents an instance of multipart upload from a HDFS file to an S3 object.
 *
 * The source is a HDFS file, and the dest is a S3 object.
 * The source is first read as smaller chunks and saved to local temp dir. Each chunk
 * is uploaded as a separate part via multi-part upload.
 *
 * Created by shawn on 9/1/14.
 */
public class MultipartUploadOutputStream extends OutputStream {
  public static final Log log = LogFactory.getLog(MultipartUploadOutputStream.class);
  final AmazonS3 s3;
  final Configuration conf;
  final ThreadPoolExecutor threadPool;
  final Progressable progressable;
  final List<Future<PartETag>> futures;
  final String tempDirname;
  final String bucketName;
  final String key;
  final String uploadId;
  final long partSize; // multipart upload chunk size.
  int partCount = 0;
  long currentPartSize = 0L;
  File currentTemp;
  DigestOutputStream currentOutput;


  public MultipartUploadOutputStream(AmazonS3 s3,
                                     String bucketName,
                                     String key,
                                     ObjectMetadata metadata,
                                     S3CopyOptions options,
                                     Configuration conf,
                                     Progressable progressable,
                                     String tempDirname) {
    this.conf = conf;
    RetryPolicy basePolicy =
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(4, 10L, TimeUnit.SECONDS);
    Map exceptionToPolicyMap = new HashMap();
    exceptionToPolicyMap.put(Exception.class, basePolicy);

    RetryPolicy methodPolicy =
        RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map methodNameToPolicyMap = new HashMap();

    methodNameToPolicyMap.put("completeMultipartUpload", methodPolicy);

    this.s3 = ((AmazonS3) RetryProxy.create(AmazonS3.class, s3, methodNameToPolicyMap));

    // S3 doesn't save the contentMD5 field in an object metadata for
    // multipart-uploaded object. So we save this info as user metadata.
    //metadata.addUserMetadata("contentmd5", metadata.getContentMD5());
    //metadata.addUserMetadata("contentlength", String.valueOf(metadata.getContentLength()));

    InitiateMultipartUploadResult result =
        this.s3.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key)
                                            .withObjectMetadata(metadata));
    this.threadPool = Utils.createDefaultExecutorService();
    this.progressable = progressable;
    this.futures = new ArrayList<Future<PartETag>>();
    this.bucketName = bucketName;
    this.key = key;
    this.uploadId = result.getUploadId();
    this.partSize = options.chunkSize;
    this.currentPartSize = 0;

    this.tempDirname = tempDirname;
    prepareTempFileToWriteTo();
  }

  /**
   * Open a temp file (at local disk) for each chunk of the input file.
   * This chunk is read from the input file, saved to this temp file.
   * Later on the temp file is used as input for a multipart upload request.
   */
  private void prepareTempFileToWriteTo() {
    try {
      this.currentPartSize = 0L;
      this.partCount++;
      String tempFilename = "multipart-" + this.partCount;
      this.currentTemp = new File(this.tempDirname, tempFilename);
      this.currentOutput =
          new DigestOutputStream(new BufferedOutputStream(new FileOutputStream(this.currentTemp)),
                                    MessageDigest.getInstance("MD5"));
      log.debug(String.format("use temp file %s for chunk %d",
                                this.currentTemp.getName(), this.partCount));
    } catch (IOException e) {
      throw new RuntimeException("Error creating temporary output stream.", e);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Error creating DigestOutputStream", e);
    }
  }


  /**
   * The local temp file now contains a chunk's worth of data. Now it's time to
   * upload this chunk using multi-part upload.
   *
   * @param isLastChunk
   * @throws IOException
   */
  private void kickOffUpload(boolean isLastChunk) throws IOException {
    this.currentOutput.close();
    byte[] digest = this.currentOutput.getMessageDigest().digest();
    String md5sum = new String(Base64.encodeBase64(digest), Charset.forName("UTF-8"));
    log.info(String.format("issue multipart upload for chunk %d, size %d",
                              this.partCount, this.currentTemp.length()));
    Future<PartETag> tag = this.threadPool.submit(
                                                     new MultipartUploadCallable(this.partCount, this.currentTemp, md5sum));
    this.futures.add(tag);
    if (!isLastChunk) {
      prepareTempFileToWriteTo();
    }
  }

  private long capacityLeft() {
    return this.partSize - this.currentPartSize;
  }

  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  public void write(byte[] b, int off, int len) throws IOException {
    long capacityLeft = capacityLeft();
    int offset = off;
    int length = len;
    while (capacityLeft < length) {
      int capacityLeftInt = (int)capacityLeft;
      this.currentOutput.write(b, offset, capacityLeftInt);
      kickOffUpload(false);
      offset += capacityLeftInt;
      length -= capacityLeftInt;
      capacityLeft = capacityLeft();
    }
    this.currentOutput.write(b, offset, length);
    this.currentPartSize += length;
  }

  public void write(int b) throws IOException {
    if (capacityLeft() < 1L) {
      kickOffUpload(false);
    }
    this.currentOutput.write(b);
    this.currentPartSize += 1L;
  }

  public void flush() {}

  public void close() {
    try {
      kickOffUpload(true);
      log.info(String.format("close() : now wait for multipart %s/%s to complete...",
                                this.bucketName, this.key));
      boolean allDone = false;
      while (!allDone) {
        allDone = true;
        for (Future future : this.futures) {
          allDone &= future.isDone();
        }
        if (this.progressable != null) {
          this.progressable.progress();
        }
        Thread.sleep(500L);
      }

      List etags = new ArrayList();
      for (Future future : this.futures) {
        etags.add(future.get());
      }
      log.debug(String.format("Will close multipart upload: %s/%s, with %d etags",
                                this.bucketName, this.key, etags.size()));
      this.s3.completeMultipartUpload(new CompleteMultipartUploadRequest(this.bucketName,
                                                                         this.key,
                                                                         this.uploadId,
                                                                         etags));
      log.info(String.format("have closed multipart upload %s/%s", this.bucketName, this.key));
    } catch (Exception e) {
      log.info(String.format("Will abort multipart upload: %s/%s", this.bucketName,
                                this.key));
      this.s3.abortMultipartUpload(new AbortMultipartUploadRequest(this.bucketName,
                                                                   this.key,
                                                                   this.uploadId));
      throw new RuntimeException(String.format("Error closing multipart upload for %s/%s",
                                                   this.bucketName, this.key), e);
    } finally {
      FileUtils.deleteLocalDir(this.tempDirname);
    }
  }

  public void abort() {
    for (Future future : this.futures) {
      future.cancel(true);
    }
    log.info(String.format("abort multipart upload %s/%s", this.bucketName, this.key));
    this.s3.abortMultipartUpload(new AbortMultipartUploadRequest(this.bucketName,
                                                                 this.key,
                                                                 this.uploadId));
  }

  /**
   * This class represent an chunk to be uploaded using multi-part upload.
   */
  private class MultipartUploadCallable implements Callable<PartETag> {
    // Multipart upload part number starts from 1.
    private final int partNumber;
    // A local temp file that stores the chunk to be uploaded.
    private final File partFile;
    // checksum of the partFile.
    private final String md5sum;

    public MultipartUploadCallable(int partNumber, File partFile, String md5sum) {
      this.partNumber = partNumber;
      this.partFile = partFile;
      this.md5sum = md5sum;
    }

    public PartETag call() throws Exception {
      int maxRetry = 5;
      int retry = 0;
      boolean uploadSuccess = true;
      InputStream is = null;
      UploadPartRequest request = null;
      UploadPartResult result = null;
      try {
        while (retry < maxRetry) {
          retry++;
          try {
            uploadSuccess = false;
            is = new ProgressableResettableBufferedFileInputStream(
                        this.partFile,
                        MultipartUploadOutputStream.this.progressable);
            request = new UploadPartRequest()
                          .withBucketName(MultipartUploadOutputStream.this.bucketName)
                          .withKey(MultipartUploadOutputStream.this.key)
                          .withUploadId(MultipartUploadOutputStream.this.uploadId)
                          .withInputStream(is)
                          .withPartNumber(this.partNumber)
                          .withPartSize(this.partFile.length())
                          .withMD5Digest(this.md5sum);
            log.info(String.format("S3 uploadPart %s/%s, part:%d attempt:%d size:%d",
                                      MultipartUploadOutputStream.this.bucketName,
                                      MultipartUploadOutputStream.this.key,
                                      this.partNumber,
                                      retry,
                                      this.partFile.length()));
            result = MultipartUploadOutputStream.this.s3.uploadPart(request);
            uploadSuccess = true;
            break;
          } catch (Exception e) {
            if (is != null) {
              is.close();
            }
            log.info(String.format("***** exception when uploadPart() part %d attempt " +
                                       "%d for %s/%s\nexception: %s",
                                      this.partNumber,
                                      retry,
                                      MultipartUploadOutputStream.this.bucketName,
                                      MultipartUploadOutputStream.this.key,
                                      e.toString()));
          }
        }
      } finally {
        try {
          if (is != null) {
            is.close();
          }
        } finally {
          log.debug("***** delete multipart file: " + this.partFile.getName());
          this.partFile.delete();
        }
      }
      if (!uploadSuccess) {
        log.info(String.format("multipart upload failed for %s/%s",
                                  MultipartUploadOutputStream.this.bucketName,
                                  MultipartUploadOutputStream.this.key));
      }
      return result.getPartETag();
    }
  }
}
