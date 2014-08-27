package com.pinterest.hdfsbackup.utils;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by shawn on 8/22/14.
 */
public class S3Utils {
  static final Log log = LogFactory.getLog(S3Utils.class);

  public static AmazonS3Client createAmazonS3Client(Configuration conf) {
    String accessKeyId = conf.get("fs.s3n.awsAccessKeyId");
    String SecretAccessKey = conf.get("fs.s3n.awsSecretAccessKey");
    AmazonS3Client s3Client;
    if ((accessKeyId != null) && (SecretAccessKey != null)) {
      s3Client = new AmazonS3Client(new BasicAWSCredentials(accessKeyId, SecretAccessKey));
      log.info("Created AmazonS3Client with conf KeyId " + accessKeyId);
    } else {
      InstanceProfileCredentialsProvider provider = new InstanceProfileCredentialsProvider();
      s3Client = new AmazonS3Client(provider);
      log.info("Created AmazonS3Client with role keyId " + provider.getCredentials().getAWSAccessKeyId());
    }
    return s3Client;
  }

  public static ObjectMetadata getObjectMetadata(AmazonS3Client s3Client,
                                                 GetObjectMetadataRequest request) {
    ObjectMetadata metadata = null;
    int retry = 3;
    boolean success = false;
    while (retry > 0) {
      retry--;
      try {
        metadata = s3Client.getObjectMetadata(request);
        success = true;
      } catch (AmazonServiceException ase) {
        log.info("Caught ServiceExcpetion", ase);
      } catch (AmazonClientException ace) {
        log.info("Caught ClientExcpetion", ace);
      } finally {
        if (!success) {
          metadata = null;
        }
      }
    }
    return metadata;
  }

  public static ObjectMetadata getObjectMetadata(AmazonS3Client s3client,
                                                 String bucket,
                                                 String key) {
    GetObjectMetadataRequest request = new GetObjectMetadataRequest(bucket, key);
    return getObjectMetadata(s3client, request);
  }

  public static String objectMetadataToString(ObjectMetadata metadata) {
    StringBuilder sb = new StringBuilder();
    sb.append("S3Object metadata:")
        .append("\ncontent-length = " + metadata.getContentLength())
        .append("\ncontent-MD5 = " + metadata.getContentMD5())
        .append("\ncontent-type = " + metadata.getContentType())
        .append("\nuser-metadata:");

    Map<String, String> userMetadata = metadata.getUserMetadata();
    for (Entry<String, String> entry : userMetadata.entrySet()) {
      sb.append(String.format("\n%s : %s", entry.getKey(), entry.getValue()));
    }
    return sb.toString();
  }

  public static String AWSServiceExceptionToString(AmazonServiceException ase) {
    StringBuilder sb = new StringBuilder();
    sb.append("Error Message:     " + ase.getMessage())
        .append("\nHTTP Status Code: " + ase.getStatusCode())
        .append("\nAWS Error Code:   " + ase.getErrorCode())
        .append("\nError Type:       " + ase.getErrorType())
        .append("\nRequest ID:       " + ase.getRequestId());
    return sb.toString();
  }
}
