package com.pinterest.hdfsbackup.cleanup_multipart;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.pinterest.hdfsbackup.utils.S3Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by shawn on 9/11/14.
 */
public class CleanupMultipartUploads {
  private static final Log log = LogFactory.getLog(CleanupMultipartUploads.class);
  Configuration conf;
  public String bucket;
  AmazonS3Client s3client;

  public void abortMultipartUpload(AmazonS3Client s3client,
                                   String bucket,
                                   MultipartUpload upload) {
    AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(bucket,
                                                                          upload.getKey(),
                                                                          upload.getUploadId());
    try {
      s3client.abortMultipartUpload(request);
    } catch (AmazonServiceException ase) {
      log.info("Service exception when abort mp: " + ase.toString());
    } catch (AmazonClientException ace) {
      log.info("Client exception when abort mp: " + ace.toString());
    }
  }

  public String multipartUploadToString(MultipartUpload upload) {
    return String.format("upload: key=%s: time=%s, storage-class=%s", upload.getKey(),
                            upload.getInitiated().toString(),
                            upload.getStorageClass());
  }

  public long getMultipartUploadSizeInBytes(AmazonS3Client s3client,
                                            String bucket,
                                            MultipartUpload upload) {
    long size = 0;
    long numParts = 0;
    ListPartsRequest request = new ListPartsRequest(bucket, upload.getKey(), upload.getUploadId());
    PartListing parts;
    boolean finished = false;

    try {
      parts = s3client.listParts(request);
      while (!finished) {
        for (PartSummary part : parts.getParts()) {
          numParts++;
          size += part.getSize();
        }
        if (!parts.isTruncated()) {
          finished = true;
        } else {
          request = new ListPartsRequest(bucket, upload.getKey(), upload.getUploadId())
                        .withPartNumberMarker(parts.getNextPartNumberMarker());
          parts = s3client.listParts(request);
        }
      }
    } catch (AmazonServiceException ase) {
      log.info("Service exception when get part size: " + ase.toString());
    } catch (AmazonClientException ace) {
      log.info("Client exception when get part size: " + ace.toString());
    }
    log.info(String.format("%s: has %d parts, %d bytes",
                              multipartUploadToString(upload),
                              numParts,
                              size));
    return size;
  }

  public List<MultipartUpload> getInProgressMultipartUploadsOlderThan(AmazonS3Client s3client,
                                                                      String bucket,
                                                                      Date lower_bound) {
    List<MultipartUpload> inProgressUploads = new ArrayList<MultipartUpload>();
    ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(bucket);
    MultipartUploadListing uploadListing;

    boolean finished = false;
    try {
      uploadListing = s3client.listMultipartUploads(request);
      while (!finished) {
        for (MultipartUpload upload : uploadListing.getMultipartUploads()) {
          //log.info(multipartUploadToString(upload));
          if (upload.getInitiated().compareTo(lower_bound) < 0) {
            inProgressUploads.add(upload);
          }
        }
        if (!uploadListing.isTruncated()) {
          finished = true;
        } else {
          request = new ListMultipartUploadsRequest(bucket)
                        .withUploadIdMarker(uploadListing.getNextUploadIdMarker())
                        .withKeyMarker(uploadListing.getNextKeyMarker());
          uploadListing = s3client.listMultipartUploads(request);
        }
      }
    } catch (Exception e) {
      log.info("Error when list in-progress multipart uploads: " + e.toString());
    }
    log.info(String.format("found %d multipart-upload older than %s",
                              inProgressUploads.size(),
                              lower_bound.toString()));
    return inProgressUploads;
  }

  public CleanupMultipartUploads(Configuration conf, String bucket) {
    this.bucket = bucket;
    this.conf = conf;
    this.s3client = S3Utils.createAmazonS3Client(this.conf);
  }

  /**
   * Clean up in-progress multipart uploads older than the lower_bound.
   *
   * @param lower_bound
   * @return
   */
  public boolean cleanupOlderThan(Date lower_bound) {
    List<MultipartUpload> uploads = getInProgressMultipartUploadsOlderThan(this.s3client,
                                                                           this.bucket,
                                                                           lower_bound);
    long totalBytes = 0;
    for (MultipartUpload upload : uploads) {
      totalBytes += getMultipartUploadSizeInBytes(this.s3client, this.bucket, upload);
      abortMultipartUpload(this.s3client, this.bucket, upload);
    }
    log.info(String.format("release %d objs, %d bytes", uploads.size(), totalBytes));
    return true;
  }

  public static void main(String args[]) {
    if (args.length < 1) {
      log.info(String.format("Usage: [command] <S3 bucket name> <days>"));
      return;
    }
    String bucket = args[0];
    Configuration conf = new Configuration();
    CleanupMultipartUploads cleanupper = new CleanupMultipartUploads(conf, bucket);
    int days = Integer.valueOf(args[1]);
    Date lower_bound = new Date(System.currentTimeMillis() - days * 24L * 60 * 60 * 1000);
    boolean ret = false;
    try {
      ret = cleanupper.cleanupOlderThan(lower_bound);
    } finally {
      System.exit(ret ? 0 : 1);
    }
  }
}
