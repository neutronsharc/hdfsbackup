package com.pinterest.hdfsbackup.utils;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
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
    int retry = 5;
    boolean success = false;
    while (retry > 0) {
      retry--;
      try {
        metadata = s3Client.getObjectMetadata(request);
        success = true;
        break;
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

  public static boolean isS3Scheme(String scheme)
  {
    return (scheme.equals("s3")) || (scheme.equals("s3n"));
  }

  public static boolean createS3Object(String objname, Configuration conf) {
    AmazonS3Client s3Client = S3Utils.createAmazonS3Client(conf);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(0);
    Path path = new Path(objname);
    URI dirUri = path.toUri();
    String bucket = dirUri.getHost();
    String key = dirUri.getPath();
    if (key.startsWith("/")) {
      key = key.substring(1);
    }
    // Sometimes an object name is "dir/" which means user wants to create
    // an S3 object to simulate a dir.
    if (objname.endsWith("/") && !key.endsWith("/")) {
      key = key + "/";
    }
    int retry = 0;
    int maxRetry = 5;
    while (retry < maxRetry) {
      retry++;
      try {
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        PutObjectResult result = s3Client.putObject(bucket, key, emptyContent, metadata);
        log.info(String.format("Have created S3 object %s/%s", bucket, key));
        return true;
      } catch (AmazonServiceException ase) {
        log.info("Server error: " + S3Utils.AWSServiceExceptionToString(ase));
      } catch (AmazonClientException ace) {
        log.info("Client error: " + ace.toString());
      }
    }
    log.info(String.format("Failed to create S3 object %s/%s", bucket, key));
    return false;
  }

  public static boolean addS3ObjectUserMetadata(AmazonS3Client s3client,
                                                String bucket,
                                                String key,
                                                Map<String, String> newuserMetadata) {
    ObjectMetadata objMetadata = S3Utils.getObjectMetadata(s3client, bucket, key);
    if (objMetadata == null) {
      log.info(String.format("unable to get obj original metadata: %s/%s", bucket, key));
      return false;
    }
    log.info(String.format("obj %s/%s: original metadata:\n%s", bucket, key,
                              objectMetadataToString(objMetadata)));
    for (Entry<String, String> ent : newuserMetadata.entrySet()) {
      String userKey = ent.getKey();
      String userValue = ent.getValue();
      log.info(String.format("add user metadata: [%s = %s] to %s/%s",
                                userKey, userValue, bucket, key));
      objMetadata.addUserMetadata(userKey, userValue);
    }
    CopyObjectRequest request = new CopyObjectRequest(bucket, key, bucket, key)
                                    .withSourceBucketName(bucket)
                                    .withSourceKey(key)
                                    .withNewObjectMetadata(objMetadata);
    int retry = 0;
    int maxRetry = 5;
    while (retry < maxRetry) {
      retry++;
      try {
        CopyObjectResult result = s3client.copyObject(request);
        if (result == null) {
          continue;
        }
        ObjectMetadata afterCopyMetadata = S3Utils.getObjectMetadata(s3client, bucket, key);
        if (afterCopyMetadata == null) {
          continue;
        }
        log.info(String.format("obj %s/%s: after-copy metadata:\n%s", bucket, key,
                                  objectMetadataToString(afterCopyMetadata)));
        boolean match = true;
        for (Entry<String, String> ent : newuserMetadata.entrySet()) {
          String userKey = ent.getKey();
          String userValue = ent.getValue();
          String afterCopyValue = afterCopyMetadata.getUserMetadata().get(userKey);
          if (!afterCopyValue.equals(userValue)) {
            log.info(String.format("Error: after copy metdata mismatch: key=%s, " +
                                      "value [%s,%s] msimtach for obj %s/%s",
                                      userKey, userValue, afterCopyValue,
                                      bucket, key));
            match = false;
            break;
          }
        }
        if (match) {
          return true;
        }
      } catch (AmazonServiceException ase) {
        log.info("Server error when set obj metadata: " + S3Utils.AWSServiceExceptionToString(ase));
      } catch (AmazonClientException ace) {
        log.info("Client error when set obj metadata: " + ace.toString());
      }
    }
    return false;
  }
}

