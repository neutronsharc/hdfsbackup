package com.pinterest.hdfsbackup.s3get;

import com.pinterest.hdfsbackup.utils.FilePairInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by shawn on 8/26/14.
 */
public class S3GetReducer implements Reducer<Text, FilePairInfo, Text, FilePairInfo> {
  private static final Log log = LogFactory.getLog(S3GetReducer.class);
  private  JobConf conf;
  private long count = 0;

  @Override
  public void close() throws IOException {
    log.info("has processed " + count + "file pairs");
  }

  @Override
  public void configure(JobConf entries) {
    this.conf = entries;
  }

  @Override
  public void reduce(Text text,
                     Iterator<FilePairInfo> iterator,
                     OutputCollector<Text, FilePairInfo> collector,
                     Reporter reporter) throws IOException {
    while (iterator.hasNext()) {
      FilePairInfo pair = (FilePairInfo) iterator.next().clone();
      log.info("get filepair: " + pair.toString());
      count++;
    }
  }
}
