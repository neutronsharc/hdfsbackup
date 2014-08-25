package com.pinterest.hdfsbackup.test;

import com.pinterest.hdfsbackup.s3tools.S3Downloader;
import com.pinterest.hdfsbackup.utils.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;

/**
 * Created by shawn on 8/23/14.
 */
public class Test {
  private static final Log log = LogFactory.getLog(Test.class);

  public static void main(String args[]) {
    log.info("run with options: " + Arrays.toString(args));

    TestOptions options = new TestOptions(args);
    if (options.helpDefined) {
      return;
    }
    Configuration conf = new Configuration();
    FileUtils.init(conf);


    String bucket = "pinterest-namenode-backup";
    String key = options.srcPath; //"test/tos3dir/100m";


    /*AmazonS3Client s3client = S3Utils.createAmazonS3Client(conf);
    ObjectMetadata metadata = S3Utils.getObjectMetadata(s3client, bucket, key);
    if (metadata == null) {
      log.info("object " + key + "not exist");
    } else {
      log.info(String.format("obj metadata = %s", S3Utils.objectMetadataToString(metadata)));
    } */
    String destDirName = options.destPath;
    boolean doChecksum = true;
    S3Downloader s3Downloader = new S3Downloader(conf);
    boolean ret = s3Downloader.DownloadFile(bucket, key, destDirName + "/" + key,
                                            doChecksum);
    log.info("download to " + destDirName + ", res = " + String.valueOf(ret));
    s3Downloader.close();
  }
}
