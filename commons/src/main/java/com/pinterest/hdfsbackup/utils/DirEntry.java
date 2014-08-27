package com.pinterest.hdfsbackup.utils;

/**
 * Created by shawn on 8/25/14.
 */
public class DirEntry {
  // the base dir name in the form of:  "<scheme>://<hose or bucket>/<parent dir>"
  public String baseDirname;
  // entryName is the obj name in the base dir.
  // File entry  "<sub dir>/objectname", without the leading '/'.
  // Dir entry:  "<sub dir>/",  with a tailing '/'.
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
    sb.append(this.baseDirname);
    sb.append(" :: ");
    sb.append(this.entryName);
    sb.append(isFile ? " :: is file, " : " :: is dir. ");
    if (isFile) {
      sb.append("file size = ").append(fileSize);
    }
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

}
