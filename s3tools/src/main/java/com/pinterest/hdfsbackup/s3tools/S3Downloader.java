package com.pinterest.hdfsbackup.s3tools;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.pinterest.hdfsbackup.utils.FileUtils;
import com.pinterest.hdfsbackup.utils.Utils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.nio.charset.Charset;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by shawn on 8/22/14.
 */
public class S3Downloader {
  static final Log log = LogFactory.getLog(S3Downloader.class);
  Configuration conf;
  AmazonS3Client s3client;
  ThreadPoolExecutor threadPool;

  public S3Downloader(Configuration conf) {
    this.conf = conf;
    this.s3client = S3Utils.createAmazonS3Client(conf);
    this.threadPool = Utils.createDefaultExecutorService();
  }

  public void close() {
    for (Runnable runnable : this.threadPool.shutdownNow()) {
    }
    this.s3client.shutdown();
  }

  public boolean DownloadFile(String bucket,
                              String key,
                              String destFilename,
                              boolean verifyChecksum) {
    if (this.s3client != null) {
      return DownloadFile(this.s3client, bucket, key, destFilename, verifyChecksum);
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
                                     boolean verifyChecksum) {
    ObjectMetadata metadata = S3Utils.getObjectMetadata(s3client, bucket, key);
    if (metadata == null) {
      log.error("fail to get object metadat : " + bucket + "/" + key);
      return false;
    }
    // TODO: check if destfile parent dir exsits.
    long startTimeMs = System.currentTimeMillis();
    boolean ret = DownloadFile(s3client, bucket, key, metadata, destFilename,
                                         verifyChecksum);
    long endTimeMs = System.currentTimeMillis();
    double bw = metadata.getContentLength() / 1000.0 / (endTimeMs - startTimeMs);
    log.info(String.format("download %d bytes, costs %d ms, bandwidth = %f MB/s",
                              metadata.getContentLength(), endTimeMs - startTimeMs, bw));
    return ret;
  }

  public boolean DownloadFile(AmazonS3Client s3client,
                                     String bucket,
                                     String key,
                                     ObjectMetadata metadata,
                                     String destFilename,
                                     boolean verifyChecksum) {
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

    while (retry < maxRetry) {
      retry++;
      if (metadata.getContentLength() == 0) {
        // the object is zero size.
        log.info(String.format("object %s/%s is zero size.", bucket, key));
        OutputStream outs = FileUtils.openHDFSOutputStream(destFilename);
        if (outs != null) {
          try {
            outs.close();
          } catch (IOException e) {}
          return true;
        }
      } else if (metadata.getContentLength() >= 1024 * 1024 * 4) {
        // The object is substantially large.  Multi-part download is needed.
        // A temp dir is needed to store interim chunks.
        log.info(String.format("object %s/%s size = %d, use multi-part download",
                                  bucket, key, metadata.getContentLength()));
        String interimDirname = "/tmp/" + UUID.randomUUID();
        if (!FileUtils.createLocalDir(interimDirname)) {
          log.info("failed to create interim dir: " + interimDirname);
          return false;
        }
        try {
          if (multipartDownload(s3client, bucket, key, metadata, destFilename, verifyChecksum,
                                   interimDirname)) {
            return true;
          }
        } finally {
          FileUtils.deleteLocalDir(interimDirname);
        }
      } else {
        // A regular-sized object.  Can download in one request.
        log.info(String.format("object %s/%s size = %d, downloaded in one object",
                                  bucket, key, metadata.getContentLength()));
        if (DownloadAsOneObject(s3client, bucket, key, metadata, destFilename, verifyChecksum)) {
          return true;
        }
      }
    }

    return false;
  }

  private S3Object downloadS3Object(AmazonS3Client s3client,
                                    GetObjectRequest request) {
    int retry = 0;
    int maxRetry = 3;
    while (retry < maxRetry) {
      retry++;
      try {
        S3Object s3Object = s3client.getObject(request);
        return s3Object;
      } catch (AmazonServiceException ase) {
        log.info("ServiceException: " + S3Utils.AWSServiceExceptionToString(ase));
        //log.info("ServiceException: ", ase);
      } catch (AmazonClientException ace) {
        log.info("ClientException: " + ace.getMessage());
      }
    }
    log.info(String.format("Error: failed to download S3 file %s/%s",
                                request.getBucketName(), request.getKey()));
    return null;
  }

  private boolean DownloadAsOneObject(AmazonS3Client s3client,
                                      String bucket,
                                      String key,
                                      ObjectMetadata metadata,
                                      String destFilename,
                                      boolean verifyChecksum) {
    S3Object s3Object = null;
    boolean result = false;
    int maxRetry = 1;
    int retry = 0;
    // Step 1:  download the s3 object
    s3Object = downloadS3Object(s3client, new GetObjectRequest(bucket, key));
    if (s3Object == null) {
      return false;
    }

    // Step 2:  copy the S3 object to buffer, and verify checksum
    Map<String, String> userMetadata = metadata.getUserMetadata();
    boolean hasChecksum = true;
    String expectedDigest = "";
    String actualDigest = "";
    long bytesCopied = 0;

    if (userMetadata.containsKey("ContentMD5".toLowerCase())) {
      expectedDigest = userMetadata.get("ContentMD5".toLowerCase());
      log.info(String.format("S3 obj %s/%s user-provide md5 = %s", bucket, key, expectedDigest));
    } else if (metadata.getContentMD5() != null) {
      expectedDigest = metadata.getContentMD5();
      log.info(String.format("S3 obj %s/%s system md5 = %s", bucket, key, expectedDigest));
    } else {
      hasChecksum = false;
      log.info(String.format("S3 obj %s/%s has no MD5 checksum", bucket, key));
      if (verifyChecksum) {
        log.info(String.format("need checksum but S3 obj %s/%s has no checksum", bucket, key));
        return false;
      }
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

    if (verifyChecksum && hasChecksum) {
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
      bytesCopied = 0;
      try {
        bytesCopied = 0;
        if (bInput == null) {
          bInput = new ByteArrayInputStream(bOutput.toByteArray());
        }
        if (fileOutStream == null) {
          fileOutStream = FileUtils.openHDFSOutputStream(destFilename);
        }
        if (fileOutStream == null || bInput == null) {
          continue;
        }
        bytesCopied = FileUtils.copyStream(bInput, fileOutStream, null);
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

  private boolean multipartDownload(AmazonS3Client s3Client,
                                    String bucket,
                                    String key,
                                    ObjectMetadata metadata,
                                    String destFilename,
                                    boolean doChecksum,
                                    String interimDirname) {
    int maxRetry = 3;
    int retry = 0;
    long partSize = 1024L * 1024 * 32;
    long objectSize = metadata.getContentLength();
    long numberOfParts = (objectSize + partSize - 1) / partSize;
    OutputStream fileOutStream = FileUtils.openHDFSOutputStream(destFilename);
    if (fileOutStream == null) {
      return false;
    }
    log.info(String.format("will multipart download %s/%s to dest %s, via temp-dir %s",
                              bucket, key, destFilename, interimDirname));
    //long maxAllowedInflightParts = 4;
    List<Future<RangeGetResult>> inflightParts = new LinkedList<Future<RangeGetResult>>();
    List<RangeGetResult> partResults = new LinkedList<RangeGetResult>();
    long currentOffset = 0;
    long endOffset;
    long finishedParts = 0;
    long partNumber = 0;

    // submit download part requests all upfront. The exec thread pool will queue
    // them up if unable to handle them all at one.
    while (currentOffset < metadata.getContentLength()) {
      endOffset = Math.min(currentOffset + partSize, objectSize) - 1;
      log.info(String.format("will get range [%d - %d] out of total %d",
                                currentOffset, endOffset, objectSize));
      String tempfilename = interimDirname + "/" + String.format("%04d", partNumber);
      inflightParts.add(this.threadPool.submit(new MultipartDownloadCallable(s3client,
                                                                                bucket,
                                                                                key,
                                                                                currentOffset,
                                                                                endOffset,
                                                                                partNumber,
                                                                                tempfilename)));
      partNumber++;
      currentOffset = endOffset + 1;
    }

    // Wait for the parts to complete.
    while (finishedParts < numberOfParts) {
      boolean anyPartDone = false;
      try {
        while (!anyPartDone) {
          for (int i = 0; i < inflightParts.size(); i++) {
            Future<RangeGetResult> part = inflightParts.get(i);
            if (part.isDone()) {
              anyPartDone = true;
              inflightParts.remove(i);
              partResults.add(part.get());
              finishedParts++;
              break;
            }
          }
        }
        if (!anyPartDone) {
          try {
            // wait for this many 100 ms.
            Thread.sleep(100L);
          } catch (InterruptedException e) {
          }
        }
      } catch (Exception e) {
        log.info(String.format("download %s/%s: error get part %d of %d",
                                  bucket, key, finishedParts, numberOfParts));
        // TODO: cancel inflight parts before error return.
        return false;
      }
    }
    Collections.sort(partResults);
    for (RangeGetResult r : partResults) {
      log.info(r.toString());
    }
    log.info(String.format("download %s/%s succeeded: %d out of %d parts",
                              bucket, key, finishedParts, numberOfParts));
    return true;
  }

  private boolean copyLocalFileToHDFSFile(String localFilename,
                                          OutputStream hdfsOutStream,
                                          MessageDigest md) {
    return false;
  }

  /**
   * Result of a range-get.
   */
  private class RangeGetResult implements Comparable<RangeGetResult> {
    S3Object object;  // the actual content of this part.
    boolean success;  // if the range-get succeeds.
    long begin; // begin offset (inclusive)
    long end;   // end offset (inclusive)
    long partNumber;  // multi-part number inside the file.
    String interimFilename;  // for large file download, each downloaded part is first saved
                              // in a temp file. Later on these interim files are
                              // concatenated into the final file.

    public RangeGetResult(long begin, long end, long partNumber, String interimFilename) {
      this.begin = begin;
      this.end = end;
      this.success = false;
      this.object = null;
      this.partNumber = partNumber;
      this.interimFilename = interimFilename;
    }

    @Override
    public int compareTo(RangeGetResult rangeGetResult) {
      if (this.begin > rangeGetResult.begin) {
        return 1;
      } else if (this.begin == rangeGetResult.begin) {
        return 0;
      } else {
        return -1;
      }
    }

    public String toString() {
      return String.format("part %03d [%d, %d] res = %s, interimfile=%s",
                              this.partNumber, this.begin, this.end,
                              this.success ? "success" : "failed",
                              this.interimFilename);
    }
  }

  public class MultipartDownloadCallable implements Callable<RangeGetResult> {

    private final AmazonS3Client s3client;
    private final String s3bucket;
    private final String s3key;
    private final long begin;
    private final long end;
    private final long partNumber;
    private final String interimFilename;

    public MultipartDownloadCallable(AmazonS3Client s3client,
                                      String s3bucket,
                                      String s3key,
                                      long begin,
                                      long end,
                                      long partNumber,
                                      String interimFilename) {
      this.s3client = s3client;
      this.s3bucket = s3bucket;
      this.s3key = s3key;
      this.begin = begin;
      this.end = end;
      this.partNumber = partNumber;
      this.interimFilename = interimFilename;
    }

    public String toString() {
      return String.format("%s/%s: range = [%d - %d], partNumber %d, save to %s",
                              this.s3bucket, this.s3key, this.begin, this.end,
                              this.partNumber, this.interimFilename);
    }

    @Override
    public RangeGetResult call() {
      //String tempFilename = this.interimFilename + "/" + this.partNumber;
      log.info(String.format("will save %s/%s [%d, %d] to temp file %s",
                                this.s3bucket, this.s3key, this.begin,
                                this.end, this.interimFilename));
      RangeGetResult result = new RangeGetResult(this.begin, this.end, this.partNumber,
                                                    this.interimFilename);
      result.success = false;
      GetObjectRequest request = new GetObjectRequest(s3bucket, s3key);
      request.setRange(this.begin, this.end);
      S3Object s3Object = downloadS3Object(this.s3client, request);
      if (s3Object == null) {
        log.info("failed to get part: " + toString());
        return result;
      }

      // TODO: retry download several times.
      OutputStream outs = null;
      InputStream ins = null;
      try {
        outs = FileUtils.openLocalOutputStream(this.interimFilename);
        ins = s3Object.getObjectContent();
        byte[] buffer = new byte[1024 * 1024];
        int len = 0;
        while ((len = ins.read(buffer)) > 0) {
          outs.write(buffer, 0, len);
        }
        result.success = true;
      } catch (IOException e) {
        log.info("error downloading part: " + toString());
      }  finally {
        try {
          if (outs != null) outs.close();
          if (ins != null) ins.close();
        } catch (IOException e) {}
      }
      return result;

    /*
          GetObjectRequest rangeObjectRequest = new GetObjectRequest(
            		bucketName, key);
            rangeObjectRequest.setRange(0, 10);
            S3Object objectPortion = s3Client.getObject(rangeObjectRequest);

            System.out.println("Printing bytes retrieved.");
            displayTextInputStream(objectPortion.getObjectContent());

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which" +
            		" means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means"+
            		" the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
     */

    }
  }

}