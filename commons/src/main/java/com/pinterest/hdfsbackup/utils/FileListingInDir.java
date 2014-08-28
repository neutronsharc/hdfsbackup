package com.pinterest.hdfsbackup.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by shawn on 8/25/14.
 *
 * This class represents the list of objects retrieved by walking through a directory.
 */
public class FileListingInDir {
  static final Log log = LogFactory.getLog(FileListingInDir.class);
  String baseDirname;
  // The key in this map is the "entry name" of an DirEntry, which is everything except for
  // the base dir name without leading '/'.
  // Example:   "<sub dir1>/<sub dir2>/filename",  "<sub dir>/"
  Map<String, DirEntry> fileEntries;
  // dirEntries map contains only empty dirs.  non-empty dirs car implicitly contained
  // in file names in fileEntries map.
  Map<String, DirEntry> dirEntries;

  long fileCount;
  long totalFileSize;
  long emptyFileCount;
  long maxFileSize;
  long dirCount;
  String maxSizeFilename;

  public FileListingInDir(String baseDirname) {
    this.baseDirname = baseDirname;
    this.fileCount = 0;
    this.totalFileSize = 0;
    this.maxFileSize = 0;
    this.maxSizeFilename = "";
    this.dirCount = 0;
    fileEntries = new TreeMap<String, DirEntry>();
    dirEntries = new TreeMap<String, DirEntry>();
  }

  public long getEntryCount() {
    return getFileEntryCount() + getDirEntryCount();
  }

  public long getFileEntryCount() {
    return fileEntries.size();
  }

  public long getDirEntryCount() {
    return dirEntries.size();
  }

  public Set<Entry<String, DirEntry>> getFileEntries() {
    return fileEntries.entrySet();
  }

  public String toString() {
    return String.format("base dir: %s, %d files, %d empty dirs, %d fileEntries, %d dirEntries\n" +
                          "%d empty files, total size = %d, max filesize = %d\n" +
                          "max size file is: %s",
                            this.baseDirname, this.fileCount, this.dirCount,
                            getFileEntryCount(), getDirEntryCount(),
                            this.emptyFileCount, this.totalFileSize, this.maxFileSize,
                            this.maxSizeFilename);
  }

  public void dump() {
    log.info("\n************\nDump file listing in dir: " + baseDirname);
    log.info(toString());
    for (Entry<String, DirEntry> e : fileEntries.entrySet()) {
      log.info(e.getValue().toString());
    }
    for (Entry<String, DirEntry> e : dirEntries.entrySet()) {
      log.info(e.getValue().toString());
    }
  }

  public boolean addEntry(DirEntry entry) {
    if (fileEntries.containsKey(entry.name())) {
      log.info("entry name already exists in file map: " + entry.name());
      return false;
    }
    if (dirEntries.containsKey(entry.name())) {
      log.info("entry name already exists in dir map: " + entry.name());
      return false;
    }

    if (entry.isFile) {
      this.fileCount++;
      this.totalFileSize += entry.fileSize;
      if (entry.fileSize > this.maxFileSize) {
        this.maxFileSize = entry.fileSize;
        this.maxSizeFilename = entry.name();
      }
      if (entry.fileSize == 0) {
        this.emptyFileCount++;
      }
      fileEntries.put(entry.name(), entry);
    } else {
      this.dirCount++;
      dirEntries.put(entry.name(), entry);
    }
    return true;
  }
}
