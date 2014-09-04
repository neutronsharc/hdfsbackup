package com.pinterest.hdfsbackup.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by shawn on 8/29/14.
 */
public class FilePairGroup implements Comparable<FilePairGroup> {
  private static final Log log = LogFactory.getLog(FilePairGroup.class);

  int groupID;
  List<FilePair> filePairs;
  long fileCount;
  long dirCount;
  long totalFileSize;
  long emptyFileCount;

  public FilePairGroup(int groupID) {
    this.groupID = groupID;
    this.filePairs = new LinkedList<FilePair>();
    this.fileCount = 0;
    this.dirCount = 0;
    this.totalFileSize = 0;
    this.emptyFileCount = 0;
    log.debug("create file group " + groupID);
  }

  public void add(FilePair pair) {
    this.filePairs.add(pair);
    if (pair.isFile.get()) {
      this.fileCount++;
      this.totalFileSize += pair.fileSize.get();
    } else {
      this.dirCount++;
    }
  }

  public long getWeight() {
    return this.fileCount * 1000 + this.dirCount * 1000 + this.totalFileSize;
  }

  /**
   * Write the list of file pairs to a partition file.
   * @param filename
   * @return
   */
  public boolean writeToFile(Path filePath, Configuration conf) {
    //Path filePath = new Path(filename);
    log.debug("will write to file: " + filePath.toString());
    SequenceFile.Writer writer;
    try {
      FileSystem fs = filePath.getFileSystem(conf);
      // the file format is:  <ID as text>  <file-pair info>
      writer = SequenceFile.createWriter(fs,
                                         conf,
                                         filePath,
                                         LongWritable.class,
                                         FilePair.class,
                                         SequenceFile.CompressionType.NONE);
    } catch (IOException e) {
      log.info("fail to open group file: " + filePath.toString());
      e.printStackTrace();
      return false;
    }
    // Sort files pairs in descending order of file size.
    sort();
    long filepairID = 0;
    try {
      for (FilePair pair : this.filePairs) {
        writer.append(new LongWritable(filepairID), pair);
        filepairID++;
      }
      return true;
    } catch (IOException e) {
      log.info("failed to write file pair to group file: " + filePath.toString());
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return false;
  }
  /**
   * Sort file pairs in descending order of size.
   * Larger files are at the front.
   */
  public void sort() {
    final Comparator<FilePair> descendingFileSizeComparator =
      new Comparator<FilePair>() {
      @Override
      public int compare(FilePair filePair, FilePair filePair2) {
        if (filePair.fileSize.get() > filePair2.fileSize.get()) {
          return -1;
        } else if (filePair.fileSize.get() == filePair2.fileSize.get()) {
          return 0;
        } else {
          return 1;
        }
      }
    };
    Collections.sort(this.filePairs, descendingFileSizeComparator);
  }

  public String briefSummary() {
    return String.format("file group %d: %d files, %d dirs, total %d bytes",
                         this.groupID, this.fileCount, this.dirCount, this.totalFileSize);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("file group %d: %d files, %d dirs, total %d bytes",
                               this.groupID, this.fileCount, this.dirCount, this.totalFileSize));
    for (FilePair pair : this.filePairs) {
      //sb.append("\n" + pair.toString());
    }
    return sb.toString();
  }

  @Override
  public int compareTo(FilePairGroup filePairGroup) {
    long weight = getWeight();
    long weight2 = filePairGroup.getWeight();
    // Smaller group returns -1.
    if (weight < weight2) {
      return -1;
    } else if (weight == weight2) {
      return 0;
    } else {
      return 1;
    }
  }
}
