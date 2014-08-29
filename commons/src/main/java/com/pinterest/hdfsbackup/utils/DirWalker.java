package com.pinterest.hdfsbackup.utils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Queue;


/**
 * Created by shawn on 8/25/14.
 *
 * This class walk through a directory, and produce a FileListingInDir object that
 * includes all objects in this base dir.
 */
public class DirWalker {
  static final Log log = LogFactory.getLog(DirWalker.class);
  Configuration conf;

  public DirWalker(Configuration conf) {
    this.conf = conf;
  }

  public FileListingInDir walkDir(String baseDirname) {
    FileListingInDir fileListing = null;
    Path baseDirPath = new Path(baseDirname);
    if (baseDirname.startsWith("s3://") || baseDirname.startsWith("s3n://")) {
      fileListing = walkS3Dir(baseDirname);
    } else {
      fileListing = walkHDFSDir(baseDirname);
    }
    log.info(fileListing.toString());
    return fileListing;
  }

  /**
   *  Walk through a S3 dir, list all objects into the file listing.
   *
   *  NOTE: In this function we assume a S3 object is named as:
   *      s3n://bucket/basedir/<optional sub dirs>/filename
   *  S3 object name decomposition:
   *      baseDirname = scheme + "://" + baseUri.getHost() + baseUri.getPath();
   *  example:
   *    a file: s3n://pinterest-namenode-backup/test/tdir/file   =>
   *    scheme "s3n",  host "pinterest-namenode-backup", path "/test/tdir/file",
   *                    key = "test/tdir/file"
   *    a dir:  "s3n://pinterest-namenode-backup/test/tdir" or,
   *            "s3n://pinterest-namenode-backup/test/tdir/"
   *    scheme "s3n",  host "pinterest-namenode-backup", path "/test/tdir/emptydir"
   *                    key = "test/tdir/emptydir/"
   * @param baseDirname
   * @return FileListing object
   */
  public FileListingInDir walkS3Dir(String baseDirname) {
    Path baseDirPath = new Path(baseDirname);
    URI baseUri = baseDirPath.toUri();
    AmazonS3Client s3Client = S3Utils.createAmazonS3Client(this.conf);
    ObjectListing objects = null;
    boolean finished = false;

    if (baseDirname.endsWith("/")) {
      baseDirname = baseDirname.substring(0, baseDirname.length() - 1);
    }
    FileListingInDir fileListing = new FileListingInDir(baseDirname);

    // prefix is the object key's prefix, excluding bucket name.
    String prefix = baseUri.getPath();
    if (prefix.length() > 1) {
      prefix = prefix.substring(1); // skip the leading "/"
    }
    log.info(String.format("will walk s3 dir: %s,  scheme=%s, host=%s, path=%s, prefix=%s",
                              baseDirname, baseUri.getScheme(), baseUri.getHost(),
                              baseUri.getPath(), prefix));
    while (!finished) {
      // 1. set bucket first. example bucket "pinterest-namenode-backup".
      // 2. set the object key inside given bucket. Example key "test/dir/filename"
      // Note: the key(prefix) doesn't have a leading "/".
      ListObjectsRequest listObjectRequest = new ListObjectsRequest()
                                                 .withBucketName(baseUri.getHost())
                                                 .withPrefix(prefix);
      // 3. set max amount of keys returned in one response body.
      if (objects != null) {
        listObjectRequest.withMaxKeys(Integer.valueOf(1000)).withMarker(objects.getNextMarker());
      }
      log.info(String.format("request to path: %s,  bucket = %s, prefix = %s",
                                baseDirname, baseUri.getHost(), prefix));
      int retryCount = 0;
      while (retryCount < 10) {
        retryCount++;
        try {
          objects = s3Client.listObjects(listObjectRequest);
        } catch (AmazonClientException e) {
          retryCount++;
          if (retryCount > 10) {
            log.info("Failed to list objects: " + e.getMessage());
            throw e;
          }
          log.info("Error listing objects: " + e.getMessage());
        }
      }
      if (objects.getObjectSummaries().size() == 0) {
        finished = true;
        continue;
      }
      for (S3ObjectSummary object : objects.getObjectSummaries()) {
        // A S3 file obj id: "s3n://bucket/basedir/<opt dir>/<opt filename>"
        //   S3 dir obj id:  "s3n://bucket/basedir", or, "s3n://bucket/basedir/<opt dir>"
        //
        // "prefix" = "basedir/<opt dir>/<opt filename>", or "basedir/<opt dir>"
        //
        // obj name = "basedir/<opt dir>/<opt filename>" or  "basedir/<opt dir>/"
        //
        // If obj name is the same as prefix, this is the only file that's listed.
        // If obj name is "prefix/",  this obj is an empty dir.
        //log.info("get object: key = '" + object.getKey() + "'");
        String objName = object.getKey();

        if (objName.equals(prefix)) {
          // User provides "baseDirname" as a file name. Only one file is listed
          String filename = objName.substring(objName.lastIndexOf('/') + 1);
          String dirname = baseDirname.substring(0, baseDirname.lastIndexOf('/'));
          log.info(String.format("list a file: basedir = %s, filename = %s",
                                    dirname, filename));
          fileListing.addEntry(new DirEntry(dirname, filename, true, object.getSize()));
          continue;
        }

        int idx = objName.indexOf(prefix);
        assert(idx == 0);
        if (prefix.length() + 1 == objName.length()) {
          // obj name is "prefix/", so prefix is an empty dir.
          assert(objName.charAt(prefix.length()) == '/');
          log.info("input basedir is empty, skip it: " + objName);
          continue;
        } else {
          // always strip the leading "/" from obj name.
          objName = objName.substring(prefix.length() + 1);
        }

        // Now objName = "basedir/<opt dir>/", or "basedir/<opt dir>/filename".
        boolean isFile = false;
        long fileSize = 0;
        if (objName.endsWith("/")) {
          isFile = false;
        } else {
          fileSize = object.getSize();
          isFile = true;
        }
        fileListing.addEntry(new DirEntry(baseDirname, objName, isFile, fileSize));
      }
      if (!objects.isTruncated())
        finished = true;
    }
    return fileListing;
  }

  public String getSuffix(String fullPathname, String prefix) {
    int idx = fullPathname.indexOf(prefix);
    if (idx >= 0) {
      String suffix = fullPathname.substring(idx + prefix.length());
      if (suffix.charAt(0) == '/') {
        suffix = suffix.substring(1);
      }
      return suffix;
    }
    return fullPathname;
  }

  public FileListingInDir walkHDFSDir(String baseDirname) {
    FileListingInDir fileListing = new FileListingInDir(baseDirname);
    Path dirPath = new Path(baseDirname);
    if (baseDirname.endsWith("/")) {
      baseDirname = baseDirname.substring(0, baseDirname.length() - 1);
    }

    Path curPath = dirPath;
    try {
      log.info("will walk HDFS dir: " + baseDirname + "\n");
      FileSystem fs = dirPath.getFileSystem(conf);
      Queue pathsToVisit = new ArrayDeque();
      pathsToVisit.add(dirPath);
      while (pathsToVisit.size() > 0) {
        curPath = (Path) pathsToVisit.remove();
        FileStatus[] statuses = fs.listStatus(curPath);
        if (statuses.length == 0) {
          // NOTE: HDFS doesn't add tailing "/" to an empty dir name.
          fileListing.addEntry(new DirEntry(baseDirname,
                                            getSuffix(curPath.toString() + "/", baseDirname),
                                            false, 0));
        }
        for (FileStatus status : statuses) {
          if (status.isDir()) {
            pathsToVisit.add(status.getPath());
          } else {
            fileListing.addEntry(new DirEntry(baseDirname,
                                              getSuffix(status.getPath().toString(), baseDirname),
                                              true, status.getLen()));
          }
        }
      }
    } catch (IOException e) {
      log.info("fail to list path: " + curPath);
    }
    return fileListing;
  }

}
