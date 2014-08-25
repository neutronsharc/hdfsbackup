package com.pinterest.hdfsbackup.utils;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.security.MessageDigest;

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

  /**
   * Open a HDFS file to read from.
   * @param filename
   * @return
   */
  public static InputStream openHDFSInputStream(String filename) {
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

  /**
   * Open a HDFS file to write to.  Overwrite if the file already exists.
   * @param filename
   * @return
   */
  public static OutputStream openHDFSOutputStream(String filename) {
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

  public static InputStream openLocalInputStream(String filename) {
    FileInputStream fin = null;
    File file = null;
    try {
      file = new File(filename);
      if (!file.exists()) {
        log.info("error input file: not exist: " + filename);
        return null;
      }
      fin = new FileInputStream(file);
      log.info("opened file " + filename + ", avail bytes = " + fin.available());
      return fin;
    } catch (FileNotFoundException e) {
      log.info("error input file: not exist: " + filename);
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static OutputStream openLocalOutputStream(String filename) {
    FileOutputStream fout = null;
    File file = null;
    try {
      file = new File(filename);
      if (!file.exists()) {
        file.createNewFile();
      }
      fout = new FileOutputStream(file);
      return fout;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static boolean createLocalDir(String dirName) {
    File dir = new File(dirName);
    if (!dir.exists()) {
      return dir.mkdirs();
    } else if (dir.isDirectory()) {
      return true;
    }
    return false;
  }

  public static boolean deleteLocalDir(String dirName) {
    File dir = new File(dirName);
    if (dir.exists()) {
      log.info("will delete local dir: " + dirName);
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(dir);
        return true;
      } catch (IOException e) {
        log.info("failed to delete dir: " + dirName);
        e.printStackTrace();
      }
      return false;
    }
    return true;
  }

  /**
   * Delete a HDFS dir recursively.
   * @param dirName
   * @param conf
   * @return
   */
  public static boolean deleteHDFSDir(String dirName, Configuration conf) {
    Path dirPath = new Path(dirName);
    boolean recursive = true;
    log.info("will delete hdfs dir: " + dirName);
    try {
      FileSystem.get(dirPath.toUri(), conf).delete(dirPath, recursive);
      return true;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  public static long copyStream(InputStream ins, OutputStream outs, MessageDigest md) {
    long copiedBytes = 0;
    byte[] buffer = new byte[1024 * 1024];
    int len = 0;
    try {
      while ((len = ins.read(buffer)) > 0) {
        if (md != null) {
          md.update(buffer, 0, len);
        }
        outs.write(buffer, 0, len);
        copiedBytes += len;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return copiedBytes;
  }

}
