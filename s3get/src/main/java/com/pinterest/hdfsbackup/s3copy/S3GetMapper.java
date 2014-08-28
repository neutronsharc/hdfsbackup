package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.utils.FilePair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

import java.io.IOException;

/**
 * Created by shawn on 8/26/14.
 */
public class S3GetMapper
    implements Mapper<LongWritable, FilePair, Text, FilePair> {
  private static final Log log = LogFactory.getLog(S3GetMapper.class);
  private static long count;
  protected JobConf conf;

  public void map(LongWritable key, FilePair filePair,
                  OutputCollector<Text, FilePair> collector,
                  Reporter reporter) throws IOException {
    log.info(String.format("input: %d  [%s]", key.get(), filePair.toString()));
    count++;
    collector.collect(new Text(key.toString()), filePair);
  }

  @Override
  public void close() throws IOException {
    log.info("has processed " + count + " filepairs");
  }

  @Override
  public void configure(JobConf entries) {
    this.conf = entries;
  }
}
