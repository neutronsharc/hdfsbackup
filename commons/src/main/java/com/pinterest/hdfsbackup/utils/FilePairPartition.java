package com.pinterest.hdfsbackup.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.PriorityQueue;

/**
 * Created by shawn on 8/29/14.
 */
public class FilePairPartition {
  static final Log log = LogFactory.getLog(FilePairGroup.class);
  int numberGroups;
  PriorityQueue<FilePairGroup> groups;

  public FilePairPartition(int numberGroups) {
    this.numberGroups = numberGroups;
    this.groups = new PriorityQueue<FilePairGroup>(numberGroups);
    for (int i = 0; i < numberGroups; i++) {
      this.groups.add(new FilePairGroup(i));
    }
  }

  public boolean createFileGroups(FileListingInDir fileList, String destDirname) {
    for (Map.Entry<String, DirEntry> e : fileList.fileEntries.entrySet()) {
      DirEntry fileEntry = e.getValue();
      FilePair pair =
          new FilePair(fileEntry.baseDirname + "/" + fileEntry.entryName,
                          destDirname == null ? "" : destDirname + fileEntry.entryName,
                          true,
                          fileEntry.fileSize);
      assert(this.groups.size() > 0);
      FilePairGroup group = this.groups.poll();
      group.add(pair);
      this.groups.add(group);
    }
    for (Map.Entry<String, DirEntry> e : fileList.dirEntries.entrySet()) {
      DirEntry dirEntry = e.getValue();
      FilePair pair =
          new FilePair(dirEntry.baseDirname + "/" + dirEntry.entryName,
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

  public void display() {
    log.info(String.format("Have %d file groups\n", groups.size()));
    for (FilePairGroup group : this.groups.toArray(new FilePairGroup[this.groups.size()])) {
      log.info(group.toString());
    }
  }
}
