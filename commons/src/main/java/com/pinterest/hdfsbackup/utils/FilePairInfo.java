package com.pinterest.hdfsbackup.utils;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by shawn on 8/26/14.
 */
public class FilePairInfo implements Writable {
  public Text srcFile;
  public Text destFile;
  public BooleanWritable isFile;
  public LongWritable fileSize;

  /**
   * This init function is needed for SequenceFileRecordReader.createValue() to
   * init an object.   *
   */
  public FilePairInfo() {
    this.srcFile = new Text();
    this.destFile = new Text();
    this.isFile = new BooleanWritable(false);
    this.fileSize = new LongWritable(0L);
  }

  public FilePairInfo(String srcFile, String destFile, boolean isFile, long fileSize) {
    this.srcFile = new Text(srcFile);
    this.destFile = new Text(destFile);
    this.isFile = new BooleanWritable(isFile);
    this.fileSize = new LongWritable(fileSize);
  }

  public String toString() {
    return String.format("[ '%s' : '%s' : %s : %d ]", this.srcFile,
                            this.destFile, this.isFile.get(), this.fileSize.get());
  }

  public Writable[] getFields() {
    return new Writable[] {this.srcFile, this.destFile, this.isFile, this.fileSize};
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    for (Writable field : getFields()) {
      field.write(dataOutput);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    for (Writable field : getFields()) {
     field.readFields(dataInput);
    }
  }

}
