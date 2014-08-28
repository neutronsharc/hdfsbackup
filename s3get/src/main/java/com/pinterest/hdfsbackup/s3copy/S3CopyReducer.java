package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.utils.FilePairInfo;
import com.pinterest.hdfsbackup.utils.SimpleExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by shawn on 8/26/14.
 */
public class S3CopyReducer implements Reducer<Text, FilePairInfo, Text, FilePairInfo> {
  private static final Log log = LogFactory.getLog(S3CopyReducer.class);
  private  JobConf conf;
  private long count = 0;
  private SimpleExecutor executor;
  private Reporter reporter;
  OutputCollector<Text, FilePairInfo> collector;
  S3CopyOptions options;
  Set<FilePairInfo> unfinishedFiles;

  @Override
  public void close() throws IOException {
    log.info("has posted " + this.count + " file pairs, wait for completion...");
    this.executor.close();
    log.info("has processed " + this.count + " file pairs");
    synchronized (this) {
      if (this.unfinishedFiles.size() > 0) {
        log.info(String.format("Error: %d files failed to copy ::",
                                  this.unfinishedFiles.size()));
        for (FilePairInfo pair : this.unfinishedFiles) {
          log.info("\t" + pair.toString());
          this.collector.collect(pair.srcFile, pair);
        }
      }
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void configure(JobConf conf) {
    this.conf = conf;
    this.options = new S3CopyOptions();
    this.options.populateFromConfiguration(conf);
    this.options.showCopyOptions();
    this.executor = new SimpleExecutor(this.options.queueSize,
                                       this.options.workerThreads);
    unfinishedFiles = new HashSet<FilePairInfo>();
  }

  @Override
  public void reduce(Text text,
                     Iterator<FilePairInfo> iterator,
                     OutputCollector<Text, FilePairInfo> collector,
                     Reporter reporter) throws IOException {
    this.reporter = reporter;
    this.collector = collector;
    int countLocal = 0;
    while (iterator.hasNext()) {
      FilePairInfo pair = ((FilePairInfo)iterator.next()).clone();
      log.info(String.format("Reducer get filepair %s: %s", text.toString(), pair.toString()));
      countLocal++;
      addUnfinishedFile(pair);
      this.executor.execute(new S3GetFileRunnable(pair, this));
    }
    log.info("posted " + countLocal + " files in one reduce round.");
    this.count += countLocal;
  }

  public boolean addUnfinishedFile(FilePairInfo pair) {
    boolean ret;
    synchronized (this) {
      ret = this.unfinishedFiles.add(pair);
    }
    log.info("add unfinished-file: " + pair.toString() + ", res=" + ret);
    return ret;
  }

  public boolean removeUnfinishedFile(FilePairInfo pair) {
    boolean ret;
    synchronized (this) {
      ret = this.unfinishedFiles.remove(pair);
    }
    log.info("mark file finished: " + pair.toString() + ", res=" + ret);
    return ret;
  }

  public synchronized boolean isFileFinished(FilePairInfo pair) {
    return !this.unfinishedFiles.contains(pair);
  }

  public void progress() {
    this.reporter.progress();
  }
}