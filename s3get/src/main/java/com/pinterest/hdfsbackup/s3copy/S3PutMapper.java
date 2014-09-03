package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.utils.FilePair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by shawn on 9/2/14.
 */
public class S3PutMapper extends S3GetMapper {
  private static final Log log = LogFactory.getLog(S3GetMapper.class);

  @Override
  public void map(LongWritable key, FilePair filePair,
                  OutputCollector<Text, FilePair> collector,
                  Reporter reporter) throws IOException {
    log.info(String.format("input: %d  [%s]", key.get(), filePair.toString()));
    this.reporter = reporter;
    this.collector = collector;
    this.bytesToCopy += filePair.fileSize.get();
    FilePair pair = filePair.clone();
    addUnfinishedFile(pair);
    this.executor.execute(new S3PutFileRunnable(pair, this, this.options));
    this.fileCount++;
  }

}