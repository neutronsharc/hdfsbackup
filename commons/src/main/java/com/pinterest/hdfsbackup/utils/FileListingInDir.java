package com.pinterest.hdfsbackup.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
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
  // the base dir name, without the trailing "/".
  String baseDirname;
  // The key in this map is the "entry name" of an DirEntry, which is everything except for
  // the base dir name without leading '/'.
  // Example:   "<sub dir1>/<sub dir2>/filename",  "<sub dir>/"
  Map<String, DirEntry> fileEntries;
  // dirEntries map contains only empty dirs.  non-empty dirs are implicitly contained
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

  public boolean compare(FileListingInDir destFileList,
                         List<Pair<DirEntry, DirEntry> > diffPairs,
                         List<Pair<DirEntry, DirEntry> > samePairs,
                         boolean ignoreDir) {
    if (getFileEntryCount() != destFileList.getFileEntryCount()) {
      log.info(String.format("source files %d != dest files %d",
                                getFileEntryCount(), destFileList.getFileEntryCount()));
      return false;
    }
    if (this.totalFileSize != destFileList.totalFileSize) {
      log.info(String.format("source file total size %d != dest file total size %d",
                             this.totalFileSize, destFileList.totalFileSize));
      return false;
    }
    long missingFiles = 0;
    long missingDirs = 0;
    for (Entry<String, DirEntry> src : getFileEntries()) {
      if (!destFileList.containsFile(src.getKey())) {
        missingFiles++;
        continue;
      }
      DirEntry srcfile = src.getValue();
      DirEntry destfile = destFileList.getFileEntry(srcfile.name());
      assert(destfile != null);
      if (!srcfile.isSameEntry(destfile)) {
        diffPairs.add(new Pair<DirEntry, DirEntry>(srcfile, destfile));
      } else {
        samePairs.add(new Pair<DirEntry, DirEntry>(srcfile, destfile));
      }
    }
    return missingDirs == 0 && missingFiles == 0 && diffPairs.size() == 0;
  }


  public boolean containsFile(String filename) {
    return this.fileEntries.containsKey(filename);
  }

  public boolean containsDir(String dirname) {
    return this.dirEntries.containsKey(dirname);
  }

  /**
   * Get the DirEntry for the given file name. This filename must exists
   * in the file entry map.
   *
   * @param filename
   * @return
   */
  public DirEntry getFileEntry(String filename) {
    if (!containsFile(filename)) return null;
    return this.fileEntries.get(filename);
  }

  public DirEntry getDirEntry(String dirname) {
    if (!containsDir(dirname)) return null;
    return this.dirEntries.get(dirname);
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

  public void display(boolean verbose) {
    log.info("======================= Base dir: " + this.baseDirname);
    log.info(String.format("%d files, %d empty files, total file size %d, %d empty dirs\n" +
                               "max-file-size %d, max-size-file: %s\n",
                              this.fileCount, this.emptyFileCount, this.totalFileSize,
                              this.dirCount, this.maxFileSize, this.maxSizeFilename));
    if (!verbose) {
      return;
    }
    dump();
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
