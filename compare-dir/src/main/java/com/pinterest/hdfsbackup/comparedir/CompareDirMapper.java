package com.pinterest.hdfsbackup.comparedir;

import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.utils.FilePair;
import com.pinterest.hdfsbackup.utils.NetworkBandwidthMonitor;
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by shawn on 9/3/14.
 */
public class CompareDirMapper implements Mapper<LongWritable, FilePair, Text, Text> {
  private static final Log log = LogFactory.getLog(CompareDirMapper.class);
  protected JobConf conf;
  protected long filePairCount = 0;
  protected SimpleExecutor executor;
  public Reporter reporter;
  public OutputCollector<Text, Text> collector;
  public S3CopyOptions options;
  //protected Map<FilePair, FilePairChecksum> unfinishedFilePairs;
  // Map source file full path to file pair checksum.
  protected Map<String, FilePairChecksum> unfinishedFilePairs;
  public long bytesToCompare = 0;

  // Network bandwidth monitor
  public NetworkBandwidthMonitor bwMonitor;
  public ScheduledExecutorService bwMonitorScheduler;
  public ScheduledFuture<?> bwMonitorHandle;

  @Override
  public void map(LongWritable key,
                  FilePair filePair,
                  OutputCollector<Text, Text> collector,
                  Reporter reporter) throws IOException {
    log.info(String.format("input: %d  [%s]", key.get(), filePair.toString()));
    this.reporter = reporter;
    this.collector = collector;
    this.bytesToCompare += filePair.fileSize.get();
    FilePair pair = filePair.clone();
    if (pair.fileSize.get() == 0) {
      log.info("Skip empty filepair: " + pair.toString());
      this.filePairCount++;
      return;
    }
    if (addUnfinishedFile(pair)) {
      this.filePairCount++;

      // compute the source file checksum.
      boolean isSource = true;
      this.executor.execute(new CompareDirRunnable(pair, isSource, this, this.options));

      // compute the dest file checksum.
      isSource = false;
      this.executor.execute(new CompareDirRunnable(pair, isSource, this, this.options));
    } else {

    }
  }

  @Override
  public void close() throws IOException {
    log.info(String.format("have posted %d file-pairs %d bytes, wait for completion...",
                              this.filePairCount, this.bytesToCompare));
    this.executor.close();
    log.info("has processed " + this.filePairCount + " file pairs");

    // Stop the bandwidth monitor.
    log.info("stop bandwidth monitor...");
    this.bwMonitorHandle.cancel(true);
    this.bwMonitorScheduler.shutdown();

    synchronized (this) {
      if (this.unfinishedFilePairs.size() > 0) {
        log.info(String.format("Error: %d file pairs checksum mismatch ::",
                                  this.unfinishedFilePairs.size()));
        for (Entry<String, FilePairChecksum> pair : this.unfinishedFilePairs.entrySet()) {
          log.info("\t" + pair.getKey().toString());
          //this.collector.collect(new Text(pair.getKey()), pair.getKey());
          this.collector.collect(new Text(pair.getKey()), new Text(pair.getValue().toString()));
        }
      }
    }

    if (this.unfinishedFilePairs.size() > 0) {
      throw new RuntimeException(String.format("%d file-pairs unable to finish",
                                                  this.unfinishedFilePairs.size()));
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
    this.unfinishedFilePairs = new TreeMap<String, FilePairChecksum>();

    this.bwMonitorScheduler = Executors.newScheduledThreadPool(1);
    this.bwMonitor = new NetworkBandwidthMonitor(this.options.networkBandwidthMonitorInterval,
                                                 this.options.workerThreads,
                                                 this.options.networkBandwidthLimit);
    this.bwMonitorHandle =
        this.bwMonitorScheduler.scheduleAtFixedRate(this.bwMonitor,
                                                    this.options.networkBandwidthMonitorInitDelay,
                                                    this.options.networkBandwidthMonitorInterval,
                                                    TimeUnit.MILLISECONDS);
    this.executor = new SimpleExecutor(this.options.queueSize,
                                       this.options.workerThreads,
                                       this.bwMonitor);
    this.executor = new SimpleExecutor(this.options.queueSize,
                                       this.options.workerThreads,
                                       this.bwMonitor);
  }

  public synchronized boolean addUnfinishedFile(FilePair pair) {
    boolean ret = false;
    String key = pair.srcFile.toString();
    if (this.unfinishedFilePairs.containsKey(key)) {
      log.error("Error: filepair already exist: " + pair.toString());
    } else {
      this.unfinishedFilePairs.put(key, new FilePairChecksum());
      ret = true;
      progress();
    }
    log.debug("add unfinished-file-pair: " + pair.toString() + ", res=" + ret);
    return ret;
  }


  public synchronized boolean removeUnfinishedFile(FilePair pair) {
    boolean ret = true;
    FilePairChecksum pairChecksum = null;
    String key = pair.srcFile.toString();
    pairChecksum = this.unfinishedFilePairs.remove(key);

    if (pairChecksum == null) {
      log.info("remove filepair, but not exist: " + pair.toString());
      ret = false;
    }
    log.info("mark file finished: " + pair.toString() + ", res=" + ret);
    return ret;
  }

  public synchronized boolean isFileFinished(FilePair pair) {
    String key = pair.srcFile.toString();
    return !this.unfinishedFilePairs.containsKey(key);
  }

  public synchronized boolean setFilePairChecksum(FilePair pair,
                                                  String checksum,
                                                  boolean isSource) {
    boolean ret = false;
    String key = pair.srcFile.toString();
    if (this.unfinishedFilePairs.containsKey(key)) {
      log.info(String.format("set file checksum %s : %s : checksum = %s",
                                isSource ? "source" : "dest",
                                isSource ? pair.srcFile.toString() : pair.destFile.toString(),
                                checksum));
      FilePairChecksum filePairChecksum = this.unfinishedFilePairs.get(key);
      assert(filePairChecksum != null);
      if (isSource) {
        ret = filePairChecksum.setSrcFileChecksum(checksum);
      } else {
        ret = filePairChecksum.setDestFileChecksum(checksum);
      }
      if (filePairChecksum.isComplete()) {
        if (filePairChecksum.match()) {
          log.info("source / dest checksum match : " + pair.toString());
          removeUnfinishedFile(pair);
        } else {
          log.info("Error: source/dest checksum mismatch : " +
                       filePairChecksum.toString() + " : " + pair.toString());
        }
      }
    } else {
      log.info("error when set checksum: filepair not found: " + pair.toString());
    }
    return ret;
  }


  public void progress() {
    this.reporter.progress();
  }

}
