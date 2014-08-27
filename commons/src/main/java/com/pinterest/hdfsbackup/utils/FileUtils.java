package com.pinterest.hdfsbackup.utils;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.*;
import java.security.MessageDigest;
import java.util.Map;

/**
 * Created by shawn on 8/22/14.
 */
public class FileUtils {
  static final Log log = LogFactory.getLog(FileUtils.class);
  private static Configuration conf = null;

  public static void init(Configuration conf) {
    setConf(conf);
  }

  private static void setConf(Configuration conf) {
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

  /**
   * Create a HDFS dir.
   * @param dirName
   * @return
   */
  public static boolean createHDFSDir(String dirName) {
    Path path = new Path(dirName);
    try {
      FileSystem fs = path.getFileSystem(FileUtils.conf);
      fs.mkdirs(path);
      return true;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Delete a HDFS dir recursively.
   * @param dirName
   * @return
   */
  public static boolean deleteHDFSDir(String dirName) {
    Path dirPath = new Path(dirName);
    boolean recursive = true;
    log.info("will delete hdfs dir: " + dirName);
    try {
      FileSystem.get(dirPath.toUri(), FileUtils.conf).delete(dirPath, recursive);
      return true;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Open a local disk file for read.
   * @param filename
   * @return
   */
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

  /**
   * Create a local disk file to write to.
   * @param filename
   * @return
   */
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

  /**
   * Create a local directory.
   * @param dirName
   * @return
   */
  public static boolean createLocalDir(String dirName) {
    File dir = new File(dirName);
    if (!dir.exists()) {
      return dir.mkdirs();
    } else if (dir.isDirectory()) {
      return true;
    }
    return false;
  }

  /**
   * Delete a local directory recursively.
   * @param dirName
   * @return
   */
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
   * Copy from input stream to output stream.  Update the digest if provided.
   * @param ins
   * @param outs
   * @param md
   * @return  number of bytes actually copied.  -1 if error occurs during copy.
   */
  public static long copyStream(InputStream ins, OutputStream outs, MessageDigest md) {
    long copiedBytes = 0;
    byte[] buffer = new byte[1024 * 1024];
    int len = 0;
    try {
      while ((len = ins.read(buffer)) > 0) {
        if (md != null) {
          md.update(buffer, 0, len);
        }
        if (outs != null) {
          outs.write(buffer, 0, len);
        }
        copiedBytes += len;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return -1;
    }
    return copiedBytes;
  }

  /**
   * Compute a HDFS file's checksum.
   * @param ins
   * @param md
   * @return
   */
  public static boolean computeHDFSDigest(InputStream ins, MessageDigest md) {
    byte[] buffer = new byte[1024 * 1024];
    try {
      int len = 0;
      while ((len = ins.read(buffer)) > 0) {
        md.update(buffer, 0, len);
      }
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   * Pair up each object in src dir with its counterpart in destination dir,
   * and save the pairs to a file specified by "pairFilePath".
   *
   * @param srcFileListing:   list of file objects in src dir.
   * @param destDirname:      destination dir name. Usually we will compare src/dest dir,
   *                   or and copy src dir to dest dir.
   * @param pairFilePath:     save file pair info to this file.
   * @param conf
   */
  public static boolean createFilePairInfoFile(FileListingInDir srcFileListing,
                                               String destDirname,
                                               Path pairFilePath,
                                               Configuration conf) {
    log.info(String.format("will create FilePair: %d files, " +
                               "%d dirs, save to '%s'",
                              srcFileListing.getFileEntryCount(),
                              srcFileListing.getDirEntryCount(),
                              pairFilePath.toString()));
    if (destDirname != null && destDirname.charAt(destDirname.length() - 1) != '/') {
      destDirname = destDirname + "/";
    }
    SequenceFile.Writer writer = null;
    try {
      FileSystem fs = pairFilePath.getFileSystem(conf);
      // the file format is:  <ID as text>  <file-pair info>
      writer = SequenceFile.createWriter(fs,
                                            conf,
                                            pairFilePath,
                                            LongWritable.class,
                                            FilePairInfo.class,
                                            SequenceFile.CompressionType.NONE);
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    long filepairID = 0;
    try {
      for (Map.Entry<String, DirEntry> e : srcFileListing.dirEntries.entrySet()) {
        DirEntry dirEntry = e.getValue();
        FilePairInfo pair =
            new FilePairInfo(dirEntry.baseDirname + "/" + dirEntry.entryName,
                             destDirname == null ? "" : destDirname + dirEntry.entryName,
                             false,
                             0);
        log.info("FilePair " + filepairID + " ::  " + pair.toString());
        writer.append(new LongWritable(filepairID), pair);
        filepairID++;
      }
      for (Map.Entry<String, DirEntry> e : srcFileListing.fileEntries.entrySet()) {
        DirEntry fileEntry = e.getValue();
        FilePairInfo pair =
            new FilePairInfo(fileEntry.baseDirname + "/" + fileEntry.entryName,
                                destDirname == null ? "" : destDirname + fileEntry.entryName,
                                true,
                                fileEntry.fileSize);
        log.info("FilePair " + filepairID + " ::  " + pair.toString());
        writer.append(new LongWritable(filepairID), pair);
        filepairID++;
      }
      return true;
    } catch (IOException e) {
      return false;
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      log.info("prepared " + filepairID + " obj-pairs");
    }
  }
}