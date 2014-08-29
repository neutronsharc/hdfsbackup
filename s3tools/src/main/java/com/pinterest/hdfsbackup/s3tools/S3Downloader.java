package com.pinterest.hdfsbackup.s3tools;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.pinterest.hdfsbackup.utils.DirEntry;
import com.pinterest.hdfsbackup.utils.FileUtils;
import com.pinterest.hdfsbackup.utils.S3Utils;
import com.pinterest.hdfsbackup.utils.Utils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
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
  S3CopyOptions options;
  Progressable progress;

  public S3Downloader(Configuration conf, S3CopyOptions options, Progressable progress) {
    this.conf = conf;
    this.s3client = S3Utils.createAmazonS3Client(conf);
    this.threadPool = Utils.createDefaultExecutorService();
    this.options = options;
    this.progress = progress;
  }

  public void setOptions(S3CopyOptions options) {
    this.options = options;
  }

  public void close() {
    for (Runnable runnable : this.threadPool.shutdownNow()) {
    }
    this.s3client.shutdown();
  }

  /**
   * Download an S3 object.
   * The srcEntry must be an valid S3 object.
   *
   * @param srcEntry
   * @param destDirname
   * @return
   */
  public boolean DownloadFile(DirEntry srcEntry, String destDirname, boolean verifyChecksum) {
    String srcFilename = srcEntry.baseDirname + "/" + srcEntry.entryName;
    String destFilename = null;
    if (destDirname != null) {
      if (destDirname.endsWith("/")) {
        destFilename = destDirname + srcEntry.entryName;
      } else {
        destFilename = destDirname + "/" + srcEntry.entryName;
      }
    }
    // get bucket and key of the object.
    Path srcPath = new Path(srcFilename);
    URI srcUri = srcPath.toUri();
    String bucket = srcUri.getHost();
    String key = srcUri.getPath();
    if (key.startsWith("/")) {
      key = key.substring(1);
    }
    return DownloadFile(bucket, key, destFilename, verifyChecksum);
  }

  /**
   * NOTE: caller should make sure the src filename is not a directory.
   *
   * @param srcFilename
   * @param destFilename
   * @param verifyChecksum
   * @return
   */
  public boolean DownloadFile(String srcFilename, String destFilename, boolean verifyChecksum) {
    // get bucket and key of the object.
    Path srcPath = new Path(srcFilename);
    URI srcUri = srcPath.toUri();
    String bucket = srcUri.getHost();
    String key = srcUri.getPath();
    if (key.startsWith("/")) {
      key = key.substring(1);
    }
    return DownloadFile(bucket, key, destFilename, verifyChecksum);
  }

  /**
   * Download a S3 object identified by "bucket/key".
   *
   * @param bucket
   * @param key
   * @param destFilename
   * @param verifyChecksum
   * @return
   */
  public boolean DownloadFile(String bucket,
                              String key,
                              String destFilename,
                              boolean verifyChecksum) {
    if (this.s3client == null) {
      log.info("Error: S3Client not initialized");
      return false;
    }
    ObjectMetadata metadata = S3Utils.getObjectMetadata(this.s3client, bucket, key);
    if (metadata == null) {
      log.error("fail to get object metadat : " + bucket + "/" + key);
      return false;
    }
    Map<String, String> userMetadata = metadata.getUserMetadata();
    if (userMetadata.containsKey("ContentLength".toLowerCase())) {
      long userProvidedLen = Long.valueOf(userMetadata.get("ContentLength".toLowerCase()));
      if (metadata.getContentLength() != userProvidedLen) {
        log.info(String.format("user-provided size %d != system size %d",
                                  userProvidedLen, metadata.getContentLength()));
        return false;
      }
    }
    long startTimeMs = System.currentTimeMillis();
    boolean ret = DownloadFile(this.s3client, bucket, key, metadata, destFilename, verifyChecksum);
    long endTimeMs = System.currentTimeMillis();
    double bw = metadata.getContentLength() / 1000.0 / (endTimeMs - startTimeMs);
    log.info(String.format("download %d bytes, costs %d ms, bandwidth = %f MB/s",
                              metadata.getContentLength(), endTimeMs - startTimeMs, bw));
    return ret;
  }

  /**
   *
   * @param s3client
   * @param bucket
   * @param key
   * @param metadata
   * @param destFilename
   * @param verifyChecksum
   * @return  true or false.
   */
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
    if (destFilename != null) {
      try {
        // Create the destination parent directory first.
        destFilePath = new Path(destFilename);
        destFs = destFilePath.getFileSystem(this.conf);
        destFs.mkdirs(destFilePath.getParent());
      } catch (Exception e) {
        log.info("failed to get filesystem for: " + destFilename);
        return false;
      }
    }
    int maxRetry = 10;
    int retry = 0;
    long expectedBytes = metadata.getContentLength();
    while (retry < maxRetry) {
      retry++;
      if (metadata.getContentLength() == 0) {
        // the object is zero size.
        log.info(String.format("object %s/%s is zero size.", bucket, key));
        if (destFilename == null) return true;
        OutputStream outs = FileUtils.openHDFSOutputStream(destFilename, this.conf);
        if (outs != null) {
          try {
            outs.close();
          } catch (IOException e) {}
          return true;
        }
      }
      // The object reaches certain size, use multi-part download.
      else if (metadata.getContentLength() >= 1024 * 1024 * 4) {
        log.info(String.format("object %s/%s size = %d, use multi-part download",
                                  bucket, key, metadata.getContentLength()));
        if (this.options.useInterimFiles) {
          // via interim files
          String interimDirname = "/tmp/" + UUID.randomUUID();
          try {
            if (!FileUtils.createLocalDir(interimDirname)) {
              log.info("failed to create interim dir: " + interimDirname);
            } else {
              if (multipartDownloadViaInterimFiles(s3client, bucket, key, metadata,
                                                   destFilename, verifyChecksum, interimDirname)
                  &&
                  (destFilename == null ||
                       FileUtils.getHDFSFileSize(destFilename,this.conf) == expectedBytes)) {
                return true;
              }
              if (destFilename != null) {
                FileUtils.deleteHDFSDir(destFilename, this.conf);
              }
            }
          } finally {
            FileUtils.deleteLocalDir(interimDirname);
          }
        } else {
          //// Not use interim files
          if (multipartDownload(s3client, bucket, key, metadata, destFilename, verifyChecksum)
              &&
              (destFilename == null ||
                   FileUtils.getHDFSFileSize(destFilename,this.conf) == expectedBytes)) {
            return true;
          }
          if (destFilename != null) {
            FileUtils.deleteHDFSDir(destFilename, this.conf);
          }
        }
      }
      // A regular-sized object.  Can download in one request.
      else {
        log.info(String.format("object %s/%s size = %d, downloaded in one object",
                                  bucket, key, metadata.getContentLength()));
        if (DownloadAsOneObject(s3client, bucket, key, metadata, destFilename, verifyChecksum)
            &&
            (destFilename == null ||
                 FileUtils.getHDFSFileSize(destFilename, this.conf) == expectedBytes)) {
          return true;
        }
        if (destFilename != null) {
          FileUtils.deleteHDFSDir(destFilename, this.conf);
        }
      }
    }
    log.info(String.format("after retry %d: failed to download %s/%s to %s",
                              retry, bucket, key, destFilename));
    return false;
  }

  /**
   * Download S3 object range given in the request.
   * @param s3client
   * @param request
   * @return
   */
  private S3Object downloadS3Object(AmazonS3Client s3client,
                                    GetObjectRequest request) {
    int retry = 0;
    int maxRetry = 30;
    while (retry < maxRetry) {
      retry++;
      try {
        S3Object s3Object = s3client.getObject(request);
        return s3Object;
      } catch (AmazonServiceException ase) {
        log.error("Server error when download S3 object:\nServiceException: "
                     + S3Utils.AWSServiceExceptionToString(ase));
      } catch (AmazonClientException ace) {
        log.error("Client error when downloading S3 object:\nClientException: "
                     + ace.getMessage());
      }
    }
    log.error(String.format("Error: failed to download S3 file %s/%s",
                                request.getBucketName(), request.getKey()));
    return null;
  }

  /**
   * Download an S3 object in a single request, without multi-part ops.
   *
   * @param s3client
   * @param bucket
   * @param key
   * @param metadata
   * @param destFilename
   * @param verifyChecksum
   * @return
   */
  private boolean DownloadAsOneObject(AmazonS3Client s3client,
                                      String bucket,
                                      String key,
                                      ObjectMetadata metadata,
                                      String destFilename,
                                      boolean verifyChecksum) {
    // Exam if checksum exists.
    Map<String, String> userMetadata = metadata.getUserMetadata();
    boolean hasChecksum = true;
    String expectedDigest = "";
    String actualDigest = "";
    if (userMetadata.containsKey("ContentMD5".toLowerCase())) {
      expectedDigest = userMetadata.get("ContentMD5".toLowerCase());
      log.info(String.format("S3 obj %s/%s user-provide md5 = %s", bucket, key, expectedDigest));
    } else if (metadata.getContentMD5() != null) {
      expectedDigest = metadata.getContentMD5();
      log.info(String.format("S3 obj %s/%s uses system-md5 = %s", bucket, key, expectedDigest));
    } else {
      hasChecksum = false;
      log.info(String.format("S3 obj %s/%s has no MD5 checksum", bucket, key));
      if (verifyChecksum) {
        log.info(String.format("need checksum but S3 obj %s/%s has no checksum", bucket, key));
        return false;
      }
    }

    S3Object s3Object;
    int maxRetry = 10;
    int retry = 0;
    long bytesCopied = 0;

    // Step 1:  download the s3 object
    s3Object = downloadS3Object(s3client, new GetObjectRequest(bucket, key));
    if (s3Object == null) {
      return false;
    }
    // Step 2:  copy the S3 object to buffer, and verify checksum
    //DigestInputStream s3DigestInput = null;
    InputStream s3ins = s3Object.getObjectContent();
    ByteArrayOutputStream bOutput = new ByteArrayOutputStream((int)(metadata.getContentLength()));
    MessageDigest md = null;
    bytesCopied = 0;
    try {
      //s3DigestInput = new DigestInputStream(s3Object.getObjectContent(),
      //                                         MessageDigest.getInstance("MD5"));
      md = MessageDigest.getInstance("MD5");
      bytesCopied = FileUtils.copyStream(s3ins, bOutput,md);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (bOutput != null) bOutput.close();
        if (s3ins != null) s3ins.close();
        log.info("close digest input...");
      } catch (Exception e) {
      }
    }
    // Make sure we get expected number of bytes, and checksum matches.
    if (bytesCopied != metadata.getContentLength()) {
      log.info(String.format("download S3 obj %s/%s size %d != expected_size %d",
                                bucket, key, bytesCopied, metadata.getContentLength()));
      return false;
    }
    if (verifyChecksum && hasChecksum) {
      actualDigest = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));
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
    if (destFilename == null) {
      return true;
    }
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
          fileOutStream = FileUtils.openHDFSOutputStream(destFilename, this.conf);
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

  /**
   * Download a S3 object using multi-part download.
   *
   * If "destFilename" is provided, the object will be saved to this destination.
   * Otherwise, the object is downloaded but not saved anywhere.
   * This is useful to verify checksum.
   *
   * @param s3client
   * @param bucket
   * @param key
   * @param metadata
   * @param destFilename
   * @param verifyChecksum
   * @return true if the object can be completed downloaded, and checksum passes.
   *          False otherwise.
   */
  private boolean multipartDownload(AmazonS3Client s3client,
                                    String bucket,
                                    String key,
                                    ObjectMetadata metadata,
                                    String destFilename,
                                    boolean verifyChecksum) {
    // 1. Get the object's checksum.
    Map<String, String> userMetadata = metadata.getUserMetadata();
    String expectedDigest = "";
    String actualDigest = "";
    if (userMetadata.containsKey("ContentMD5".toLowerCase())) {
      expectedDigest = userMetadata.get("ContentMD5".toLowerCase());
      log.info(String.format("S3 obj %s/%s user-provide md5 = %s", bucket, key, expectedDigest));
    } else if (metadata.getContentMD5() != null) {
      expectedDigest = metadata.getContentMD5();
      log.info(String.format("S3 obj %s/%s system md5 = %s", bucket, key, expectedDigest));
    } else {
      log.info(String.format("S3 obj %s/%s has no MD5 checksum", bucket, key));
      if (verifyChecksum) {
        log.info(String.format("need checksum but S3 obj %s/%s has no checksum", bucket, key));
        return false;
      }
    }
     // 2. submit download part requests.
    List<Future<RangeGetResult>> inflightParts = new LinkedList<Future<RangeGetResult>>();
    List<RangeGetResult> partResults = new LinkedList<RangeGetResult>();
    long currentOffset = 0;
    long endOffset;
    long finishedParts = 0;
    long partNumber = 0;
    long partSize = this.options.chunkSize; //1024L * 1024 * 32;
    long objectSize = metadata.getContentLength();
    long numberOfParts = (objectSize + partSize - 1) / partSize;
    long maxInflightParts = 1;
    MessageDigest md = null;
    log.info(String.format("will multipart download %s/%s at chunk size %d, total %d bytes" +
                               " in %d parts to dest : %s",
                              bucket, key, partSize, objectSize, numberOfParts, destFilename));
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return false;
    }
    boolean partFailed = false;
    long bytesCopied = 0;
    OutputStream destOutStream = null;
    if (destFilename != null) {
      destOutStream = FileUtils.openHDFSOutputStreamWithProgress(destFilename, this.conf,
                                                                    this.progress);
      if (destOutStream == null) {
        log.info("Unable to open dest file: " + destFilename);
        return false;
      }
    } else {
      log.info(String.format("will read object %s/%s, but not save to anywhere", bucket, key));
    }
    while (finishedParts < numberOfParts) {
      if (currentOffset < objectSize && inflightParts.size() < maxInflightParts) {
        endOffset = Math.min(currentOffset + partSize, objectSize) - 1;
        log.info(String.format("will get part %d range [%d - %d] / %d for %s, no interim file",
                                  partNumber, currentOffset, endOffset, objectSize, key));
        inflightParts.add(this.threadPool.submit(new MultipartDownloadCallable(s3client,
                                                                                  bucket,
                                                                                  key,
                                                                                  currentOffset,
                                                                                  endOffset,
                                                                                  partNumber,
                                                                                  null)));
        partNumber++;
        currentOffset = endOffset + 1;
        continue;
      }
      Future<RangeGetResult> part = inflightParts.get(0);
      if (part.isDone()) {
        this.progress.progress();
        inflightParts.remove(0);
        finishedParts++;
        RangeGetResult r = null;
        try {
          r = part.get();
          partResults.add(r);
          if (!r.success) {
            partFailed = true;
            break;
          } else {
            long len = FileUtils.copyStream(r.object.getObjectContent(), destOutStream, md);
            r.object.getObjectContent().close();
            if (len == (r.end - r.begin + 1)) {
              bytesCopied += len;
              //log.info(String.format("got obj %s/%s: range [%d, %d]", bucket, key, r.begin,
              //                           r.end));
            } else {
              log.info(String.format("fail to obj %s/%s: range [%d, %d]",
                                        bucket, key, r.begin, r.end));
              partFailed = true;
              break;
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          try {
            if (r != null && r.object != null && r.object.getObjectContent() != null) {
              r.object.getObjectContent().close();
            }
          } catch (IOException exp) {}
          partFailed = true;
          break;
        }
      } else {
        try {
          // wait for this many ms.
          Thread.sleep(100L);
        } catch (InterruptedException e) {}
      }
    }
    try {
      if (destOutStream != null) {
        destOutStream.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    boolean ret = true;
    if (partFailed || bytesCopied != objectSize) {
      cancelMultipartRequest(inflightParts);
      log.info(String.format("download %s/%s failed, got bytes %d of %d",
                                bucket, key, bytesCopied, objectSize));
      ret = false;
    } else if (verifyChecksum) {
      actualDigest = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));
      if (!actualDigest.equals(expectedDigest)) {
        log.info(String.format("download %s/%s: checksum mismatch", bucket, key));
        ret = false;
      } else {
        log.info(String.format("download %s/%s success and checksum success", bucket, key));
      }
    }
    if (!ret) {
      log.info(String.format("download %s/%s failed, delete dest file %s",
                                bucket, key, destFilename));
      FileUtils.deleteHDFSDir(destFilename, this.conf);
    }
    return ret;
  }


  private boolean multipartDownloadViaInterimFiles(AmazonS3Client s3client,
                                                   String bucket,
                                                   String key,
                                                   ObjectMetadata metadata,
                                                   String destFilename,
                                                   boolean verifyChecksum,
                                                   String interimDirname) {
    // 1. Get the object's checksum.
    Map<String, String> userMetadata = metadata.getUserMetadata();
    String expectedDigest = "";
    String actualDigest = "";
    if (userMetadata.containsKey("ContentMD5".toLowerCase())) {
      expectedDigest = userMetadata.get("ContentMD5".toLowerCase());
      log.info(String.format("S3 obj %s/%s user-provide md5 = %s", bucket, key, expectedDigest));
    } else if (metadata.getContentMD5() != null) {
      expectedDigest = metadata.getContentMD5();
      log.info(String.format("S3 obj %s/%s system md5 = %s", bucket, key, expectedDigest));
    } else {
      log.info(String.format("S3 obj %s/%s has no MD5 checksum", bucket, key));
      if (verifyChecksum) {
        log.info(String.format("need checksum but S3 obj %s/%s has no checksum", bucket, key));
        return false;
      }
    }
    log.info(String.format("will multipart download %s/%s to dest %s, via temp-dir %s",
                              bucket, key, destFilename, interimDirname));
    // 2. submit download part requests all upfront. The exec thread pool will queue
    // them up if unable to handle them all at one.
    List<Future<RangeGetResult>> inflightParts = new LinkedList<Future<RangeGetResult>>();
    List<RangeGetResult> partResults = new LinkedList<RangeGetResult>();
    long currentOffset = 0;
    long endOffset;
    long finishedParts = 0;
    long partNumber = 0;
    long partSize = 1024L * 1024 * 16;
    long objectSize = metadata.getContentLength();
    long numberOfParts = (objectSize + partSize - 1) / partSize;

    while (currentOffset < objectSize) {
      endOffset = Math.min(currentOffset + partSize, objectSize) - 1;
      String tempfilename = interimDirname + "/" + String.format("multipart-down-%04d", partNumber);
      inflightParts.add(this.threadPool.submit(new MultipartDownloadCallable(s3client,
                                                                                bucket,
                                                                                key,
                                                                                currentOffset,
                                                                                endOffset,
                                                                                partNumber,
                                                                                tempfilename)));
      log.info(String.format("will get part %d range [%d - %d] / %d for %s, interim file %s",
                                partNumber, currentOffset, endOffset,
                                objectSize, key, tempfilename));
      partNumber++;
      currentOffset = endOffset + 1;
    }
    // 3. Wait for the parts to complete.
    while (finishedParts < numberOfParts) {
      boolean anyPartDone = false;
      try {
        while (!anyPartDone) {
          for (int i = 0; i < inflightParts.size(); i++) {
            Future<RangeGetResult> part = inflightParts.get(i);
            if (part.isDone()) {
              this.progress.progress();
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
            // wait for this many ms.
            Thread.sleep(100L);
          } catch (InterruptedException e) {
          }
        }
      } catch (Exception e) {
        log.info(String.format("download %s/%s: error get part %d of %d",
                                  bucket, key, finishedParts, numberOfParts));
        cancelMultipartRequest(inflightParts);
        return false;
      }
    }

    // 4. Write interim parts to destination, compute checksum in the flight.
    Collections.sort(partResults);
    boolean partFailed = false;
    for (RangeGetResult r : partResults) {
      if (!r.success) {
        log.info(String.format("download %s/%s: failed part: %s", bucket, key, r.toString()));
        partFailed = true;
      }
    }
    if (partFailed) {
      log.info(String.format("download %s/%s failed", bucket, key));
      return false;
    }
    long bytesCopied = 0;
    int maxRetry = 3;
    int retry = 0;
    MessageDigest md = null;
    OutputStream destOutStream = null;
    while (retry < maxRetry) {
      retry++;
      bytesCopied = 0;
      try {
        if (destFilename != null && destFilename.length() > 0) {
          destOutStream = FileUtils.openHDFSOutputStreamWithProgress(destFilename, this.conf,
                                                                        this.progress);
          if (destOutStream == null) {
            continue;
          }
        }
        md = MessageDigest.getInstance("MD5");
        for (RangeGetResult r : partResults) {
          log.info(r.toString());
          long len = copyLocalFileToHDFSFile(r.interimFilename, destOutStream, md);
          if (len < 0) {
            log.info("Error when copying multipart chunk to dest: " + r.toString());
            break;
          } else {
            bytesCopied += len;
          }
        }
        if (bytesCopied == objectSize) {
          break;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (destOutStream != null) {
      try {
        destOutStream.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    boolean ret = false;
    actualDigest = new String(Base64.encodeBase64(md.digest()), Charset.forName("UTF-8"));
    if (bytesCopied != objectSize) {
      log.info(String.format("download %s/%s failed: copied %d of %d",
                                bucket, key, bytesCopied, objectSize));
    } else if (verifyChecksum) {
      if (expectedDigest.equals(actualDigest)) {
        log.info(String.format("download %s/%s success and checksum success, copied %d bytes, " +
                                   "%d out of %d parts",
                                  bucket, key, bytesCopied, finishedParts, numberOfParts));
        ret = true;
      } else {
        log.info(String.format("download %s/%s checksum mismatch",bucket, key));
      }
    } else {
      log.info(String.format("download %s/%s success no checksum, copied %d bytes, " +
                                 "%d out of %d parts",
                                bucket, key, bytesCopied, finishedParts, numberOfParts));
      ret = true;
    }
    if (!ret) {
      log.info(String.format("download %s/%s failed, delete dest file %s",
                                bucket, key, destFilename));
      FileUtils.deleteHDFSDir(destFilename, this.conf);
    }
    return ret;
  }

  private void cancelMultipartRequest(List<Future<RangeGetResult>> parts) {
    for (Future f : parts) {
      f.cancel(true);
    }
  }

  private long copyLocalFileToHDFSFile(String localFilename,
                                       OutputStream hdfsOutStream,
                                       MessageDigest md) {
    InputStream ins = FileUtils.openLocalInputStream(localFilename);
    try {
      if (ins != null) {
        return FileUtils.copyStream(ins, hdfsOutStream, md);
      } else {
        return -1;
      }
    } finally {
      if (ins != null) {
        try {
          ins.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
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
      if (this.interimFilename == null) {
        result.object = s3Object;
        result.success = true;
        return result;
      }
      int retry = 0;
      int maxRetry = 3;
      while (retry < maxRetry) {
        retry++;
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
          //log.info(String.format("saved %s/%s [%d, %d] to temp file %s in attempt %d",
          //                          this.s3bucket, this.s3key, this.begin,
          //                          this.end, this.interimFilename, retry));
          return result;
        } catch (IOException e) {
          log.info("error saving part to interim file: " + toString()
                       + " :: " + this.interimFilename);
        } finally {
          try {
            if (outs != null) outs.close();
            if (ins != null) ins.close();
          } catch (IOException e) {
          }
        }
      }
      return result;
    }
  }

}