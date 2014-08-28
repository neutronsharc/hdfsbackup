package com.pinterest.hdfsbackup.test;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.s3tools.S3Downloader;
import com.pinterest.hdfsbackup.utils.DirWalker;
import com.pinterest.hdfsbackup.utils.FileListingInDir;
import com.pinterest.hdfsbackup.utils.S3Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;
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

    System.out.println(System.getProperty("java.class.path"));

    ///////////////////
    // a file uri must be:  "file:///xxxx/xxx".
    Path localPath = new Path("file:///tmp/dir1/");
    URI baseUri = localPath.toUri();
    Path s3Path = new Path("s3n://pinterest-namenode-backup/test/tos3dir/1g");
    URI s3Uri = s3Path.toUri();
    Path hdfsPath = new Path("/user/shawn/test");
    URI hdfsUri = hdfsPath.toUri();

    Configuration conf = new Configuration();

    //testS3(conf, options);
    testDir(conf, options);

  }

  private static void testS3(Configuration conf, TestOptions options) {
    String key = options.srcPath; //"test/tos3dir/100m";
    String bucket = "pinterest-namenode-backup";
    AmazonS3Client s3client = S3Utils.createAmazonS3Client(conf);
    ObjectMetadata metadata = S3Utils.getObjectMetadata(s3client, bucket, key);
    if (metadata == null) {
      log.info("object " + key + "not exist");
    } else {
      log.info(String.format("obj metadata = %s", S3Utils.objectMetadataToString(metadata)));
    }
    String destDirName = options.destPath;
    boolean doChecksum = true;

    S3Downloader s3Downloader = new S3Downloader(conf, new S3CopyOptions());
    boolean ret = s3Downloader.DownloadFile(bucket, key,
                                            (destDirName == null) ? destDirName :
                                              destDirName + "/" + key,
                                            options.verifyChecksum);
    log.info("download to " + destDirName + ", res = " + String.valueOf(ret));
    s3Downloader.close();
  }

  private static void testDir(Configuration conf, TestOptions options) {
    DirWalker dirWalker = new DirWalker(conf);
    FileListingInDir filelist = dirWalker.walkDir(options.srcPath);
    if (options.verbose) {
      filelist.dump();
    }
  }
}
