package com.pinterest.hdfsbackup.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.*;

import java.util.Collections;

/**
 * Created by shawn on 8/29/14.
 */
public class FilePairPartition {
  static final Log log = LogFactory.getLog(FilePairPartition.class);
  int numberGroups;
  PriorityQueue<FilePairGroup> groups;

  public FilePairPartition(int numberGroups) {
    this.numberGroups = numberGroups;
    this.groups = new PriorityQueue<FilePairGroup>(numberGroups);
    for (int i = 0; i < numberGroups; i++) {
      this.groups.add(new FilePairGroup(i));
    }
  }

  public boolean createFileGroups(FileListingInDir fileList,
                                  String destDirname,
                                  boolean includeDir) {
    // Make sure dest dir always have a trailing "/".
    if (destDirname != null && !destDirname.endsWith("/")) {
      destDirname = destDirname + "/";
    }

    // Sort file entries in descending order of file size.
    List<DirEntry> entries = new ArrayList<DirEntry>((int)(fileList.getFileEntryCount()));
    for (Map.Entry<String, DirEntry> e : fileList.fileEntries.entrySet()) {
      DirEntry ent = e.getValue();
      entries.add(ent);
    }
    Collections.sort(entries);
    for (DirEntry fileEntry : entries) {
      FilePair pair = new FilePair(fileEntry.baseDirname + "/" + fileEntry.entryName,
                                    destDirname == null ? "" : destDirname + fileEntry.entryName,
                                    true,
                                    fileEntry.fileSize);
      assert(this.groups.size() > 0);
      FilePairGroup group = this.groups.poll();
      group.add(pair);
      this.groups.add(group);
    }
    if (!includeDir) return true;
    // No need to sort dir entries since they are all empty dirs.
    for (Map.Entry<String, DirEntry> e : fileList.dirEntries.entrySet()) {
      DirEntry dirEntry = e.getValue();
      FilePair pair = new FilePair(dirEntry.baseDirname + "/" + dirEntry.entryName,
                                    destDirname == null ? "" : destDirname + dirEntry.entryName,
                                    false,
                                    0);
      assert(this.groups.size() > 0);
      FilePairGroup group = this.groups.poll();
      group.add(pair);
      this.groups.add(group);
    }
    return true;
  }

  public boolean createFileGroups(FileListingInDir fileList, String destDirname) {
    boolean includeDir = true;
    return createFileGroups(fileList, destDirname, includeDir);
  }

  public boolean writeGroupsToFiles(Path baseDirPath, Configuration conf) {
    for (FilePairGroup group : this.groups.toArray(new FilePairGroup[this.groups.size()])) {
      String filename = String.format("filegroup-%03d", group.groupID);
      if (!group.writeToFile(new Path(baseDirPath, filename), conf)) {
        log.info(String.format("failed to write group %d to file %s", group.groupID, filename));
        return false;
      }
      log.debug(String.format("write group %d to file %s", group.groupID, filename));
    }
    return true;
  }

  public void display(boolean verbose) {
    log.info(String.format("Have %d file groups\n", groups.size()));
    for (FilePairGroup group : this.groups.toArray(new FilePairGroup[this.groups.size()])) {
      if (verbose) {
        log.info(group.toString());
      } else {
        log.info(group.briefSummary());
      }
    }
  }
}
