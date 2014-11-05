package com.pinterest.hdfsbackup.utils;

/**
 * Created by shawn on 8/25/14.
 */
public class DirEntry implements Comparable {
  // the base dir name in the form of:  "<scheme>://<host or bucket>/<parent dir>",
  // without the tailing '/'.
  public String baseDirname;
  // entryName is the obj name in the base dir.
  // File entry  "<sub dir>/objectname", without the leading '/'.
  // Dir entry:  "<sub dir>/",  without leading "/", and with a trailing '/'.
  public String entryName;

  public boolean isFile;
  public long fileSize;

  public DirEntry(String baseDirname, String entryName, boolean isFile, long fileSize) {
    this.baseDirname = baseDirname;
    this.entryName = entryName;
    this.isFile = isFile;
    this.fileSize = fileSize;
  }

  public String name() {
    return entryName;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.baseDirname)
      .append(" | ")
      .append(this.entryName)
      .append(this.isFile ? " | file | " : " | dir | ")
      .append(String.valueOf(this.fileSize));
    return sb.toString();
  }

  public boolean isSameEntry(DirEntry ent2) {
    if (!ent2.name().equalsIgnoreCase(name()) ||
            ent2.isFile != this.isFile ||
            (ent2.isFile && ent2.fileSize != this.fileSize)) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(Object object) {
    // In descending order of file size
    DirEntry ent2 = (DirEntry)object;
    if (this.fileSize > ent2.fileSize) {
      return -1;
    } else if (this.fileSize == ent2.fileSize) {
      return 0;
    } else {
      return 1;
    }
  }
}
