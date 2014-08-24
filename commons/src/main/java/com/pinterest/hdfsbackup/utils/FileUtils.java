package com.pinterest.hdfsbackup.utils;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * Created by shawn on 8/22/14.
 */
public class FileUtils {
  static final Log log = LogFactory.getLog(FileUtils.class);
  private static Configuration conf = null;

  public static void init(Configuration conf) {
    setConf(conf);
  }
  public static void setConf(Configuration conf) {
    FileUtils.conf = conf;
  }

  public static InputStream openInputStream(String filename) {
    InputStream istream = null;
    try {
      Path filePath = new Path(filename);
      FileSystem fs = filePath.getFileSystem(FileUtils.conf);
      istream = fs.open(filePath);
    } catch (IOException e) {
      log.info("failed to open input file " + filename, e);
    } finally {

    }
    return istream;
  }

  public static OutputStream openOutputStream(String filename) {
    OutputStream ostream = null;
    boolean overwrite = true;
    try {
      Path outputFilePath = new Path(filename);
      FileSystem outputFs = outputFilePath.getFileSystem(FileUtils.conf);
      ostream = outputFs.create(outputFilePath, overwrite);
    } catch (IOException e) {
      log.info("failed to open output file " + filename, e);
      ostream = null;
    } finally {
    }
    return ostream;
  }

}
