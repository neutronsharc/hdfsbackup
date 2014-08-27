package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.utils.FilePairInfo;
import com.pinterest.hdfsbackup.utils.SimpleExecutor;
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
public class S3CopyReducer implements Reducer<Text, FilePairInfo, Text, FilePairInfo> {
  private static final Log log = LogFactory.getLog(S3CopyReducer.class);
  private  JobConf conf;
  private long count = 0;
  private SimpleExecutor executor;
  private Reporter reporter;

  public int fileQueueSize;
  public int numberWorkerThreads;
  public long multipartChunkSize;
  public boolean userMultipart;

  @Override
  public void close() throws IOException {
    log.info("has posted " + count + " file pairs");
    this.executor.close();
    log.info("has finished downloading " + count + " file pairs");
  }

  @Override
  public void configure(JobConf conf) {
    this.conf = conf;
    this.fileQueueSize = conf.getInt("s3copy.queuesize", 1000);
    this.numberWorkerThreads = conf.getInt("s3copy.numberthreads", 10);
    this.userMultipart = conf.getBoolean("s3copy.multipart", true);
    this.multipartChunkSize = conf.getInt("s3copy.chunksizemb", 32) * (1024L * 1024);

    this.executor = new SimpleExecutor(this.fileQueueSize, this.numberWorkerThreads);
  }

  @Override
  public void reduce(Text text,
                     Iterator<FilePairInfo> iterator,
                     OutputCollector<Text, FilePairInfo> collector,
                     Reporter reporter) throws IOException {
    this.reporter = reporter;
    while (iterator.hasNext()) {
      FilePairInfo pair = (FilePairInfo) iterator.next();
      log.info(String.format("get filepair %s: [%s]", text.toString(), pair.toString()));
      count++;
    }
  }

  public void progress() {
    this.reporter.progress();
  }
}