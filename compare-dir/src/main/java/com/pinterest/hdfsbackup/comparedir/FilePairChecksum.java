package com.pinterest.hdfsbackup.comparedir;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by shawn on 9/3/14.
 */
public class FilePairChecksum {
  private static final Log log = LogFactory.getLog(FilePairChecksum.class);

  public String srcFileChecksum = null;
  public String destFileChecksum = null;

  public boolean setSrcFileChecksum(String checksum) {
    if (this.srcFileChecksum != null) {
      log.error("Error: src file checksum already exist!");
      return false;
    }
    this.srcFileChecksum = checksum;
    return true;
  }

  public boolean setDestFileChecksum(String checksum) {
    if (this.destFileChecksum != null) {
      log.error("Error: dest file checksum already exist!");
    }
    this.destFileChecksum = checksum;
    return true;
  }

  public FilePairChecksum() {}

  public boolean isComplete() {
    return this.srcFileChecksum != null && this.destFileChecksum != null;
  }

  public boolean match() {
    return this.srcFileChecksum.equals(this.destFileChecksum);
  }

  public String toString() {
    return String.format("[%s | %s]", srcFileChecksum, destFileChecksum);
  }

}