package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.utils.FilePair;
import com.pinterest.hdfsbackup.utils.SimpleExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by shawn on 8/26/14.
 */
public class S3GetMapper implements Mapper<LongWritable, FilePair, Text, FilePair> {
  private static final Log log = LogFactory.getLog(S3GetMapper.class);
  protected JobConf conf;
  protected long fileCount = 0;
  protected SimpleExecutor executor;
  public Reporter reporter;
  public OutputCollector<Text, FilePair> collector;
  public S3CopyOptions options;
  protected Set<FilePair> unfinishedFiles;
  public long bytesToCopy = 0;

  @Override
  public void close() throws IOException {
    log.info(String.format("have posted %d files %d bytes, wait for completion...",
                              this.fileCount, this.bytesToCopy));
    this.executor.close();
    log.info("has processed " + this.fileCount + " file pairs");
    synchronized (this) {
      if (this.unfinishedFiles.size() > 0) {
        log.info(String.format("Error: %d files failed to copy ::",
                                  this.unfinishedFiles.size()));
        for (FilePair pair : this.unfinishedFiles) {
          log.info("\t" + pair.toString());
          this.collector.collect(pair.srcFile, pair);
        }
      }
    }

    if (this.unfinishedFiles.size() > 0) {
      throw new RuntimeException(String.format("%d files unable to finish",
                                                  this.unfinishedFiles.size()));
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
    unfinishedFiles = new HashSet<FilePair>();
  }

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
    this.executor.execute(new S3GetFileRunnable(pair, this, this.options));
    this.fileCount++;
  }

  public boolean addUnfinishedFile(FilePair pair) {
    boolean ret;
    synchronized (this) {
      ret = this.unfinishedFiles.add(pair);
      progress();
    }
    log.info("add unfinished-file: " + pair.toString() + ", res=" + ret);
    return ret;
  }

  public boolean removeUnfinishedFile(FilePair pair) {
    boolean ret;
    synchronized (this) {
      ret = this.unfinishedFiles.remove(pair);
    }
    log.info("mark file finished: " + pair.toString() + ", res=" + ret);
    return ret;
  }

  public synchronized boolean isFileFinished(FilePair pair) {
    return !this.unfinishedFiles.contains(pair);
  }

  public void progress() {
    this.reporter.progress();
  }


}