package com.pinterest.hdfsbackup.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by shawn on 11/4/14.
 */
public class NetworkBandwidthMonitor implements Runnable {
  private static final Log log = LogFactory.getLog(NetworkBandwidthMonitor.class);

  // Monitor interval in milli-seconds
  private long monitorInterval;

  // In the last interval, pause worker thread this many milli-seconds.
  private long sleepTimeInLastInterval;
  private long perWorkerSleepTimeInLastInterval;

  private long savedSleepTimeInLastInterval;

  // How many worker threads are throttled by this monitor.
  private long numberOfWorkers;

  // network bandwidth limit in MB/s
  private double bwLimit;

  // How many bytes are copied in last interval.
  private long bytesCopiedInLastInterval;

  public NetworkBandwidthMonitor(long monitorInterval, long numWorkers, double bwLimit) {
    this.monitorInterval = monitorInterval;
    this.numberOfWorkers = numWorkers;
    this.bwLimit = bwLimit;
    this.sleepTimeInLastInterval = 0L;
    this.perWorkerSleepTimeInLastInterval = 0L;
    this.bytesCopiedInLastInterval = 0L;
  }

  public synchronized long getBytesCopiedInLastInterval() {
    return this.bytesCopiedInLastInterval;
  }

  public synchronized void incBytesCopiedInLastInterval(long v) {
    this.bytesCopiedInLastInterval += v;
  }

  public synchronized void setBytesCopiedInLastInterval(long v) {
    this.bytesCopiedInLastInterval = v;
  }

  public synchronized void setSleepTimeInLastInterval(long v) {
    this.sleepTimeInLastInterval = v;
  }

  public synchronized long getSleepTimeInLastInterval() {
    return this.sleepTimeInLastInterval;
  }

  public synchronized void updateSleepTimeInLastInterval(long v) {
    this.sleepTimeInLastInterval += v;
  }

  public long getPerWorkerSleepTimeInLastInterval() {
    return this.perWorkerSleepTimeInLastInterval;
  }

  public synchronized void setPerWorkerSleepTimeInLastInterval(long v) {
    this.perWorkerSleepTimeInLastInterval = v;
  }

  public synchronized void setNumberOfWorkers(long v) {
    this.numberOfWorkers = v;
  }

  public synchronized long getNumberOfWorkers() {
    return this.numberOfWorkers;
  }

  public long getSavedSleepTimeInLastInterval() {
    return this.savedSleepTimeInLastInterval;
  }

  @Override
  public void run() {
    //long timeUsedToCopy = this.monitorInterval - getSleepTimeInLastInterval();
    long timeUsedToCopy = this.monitorInterval;
                              //getPerWorkerSleepTimeInLastInterval();
    long bytesCopied = getBytesCopiedInLastInterval();
    setBytesCopiedInLastInterval(0);
    double bwUsed =  bytesCopied / (1024.0 * 1024) / (timeUsedToCopy / 1000.0);
    long numWorkers = getNumberOfWorkers();

    log.info(String.format("bw-monitor: bytesCopied = %d, " +
                               "remaining worker threads = %d, " +
                               "perWorkerSleep = %d ms, " +
                               "timeUsedToCopy = %d ms, " +
                               "total sleepInLastInterval = %d ms, " +
                               "remaining sleeptime = %d ms, " +
                               "usedBW = %f, " +
                               "bwLimit = %f",
                              bytesCopied,
                              numWorkers,
                              getPerWorkerSleepTimeInLastInterval(),
                              timeUsedToCopy,
                              this.savedSleepTimeInLastInterval,
                              getSleepTimeInLastInterval(),
                              bwUsed,
                              this.bwLimit));

    if (bwUsed > this.bwLimit) {
      double fraction = (bwUsed - this.bwLimit) / bwUsed;
      long toSleepTime = (long)(fraction * this.monitorInterval);
      long perWorkerSleepTime = toSleepTime / (numWorkers > 0 ? numWorkers : 1);
      setSleepTimeInLastInterval(toSleepTime);
      setPerWorkerSleepTimeInLastInterval(perWorkerSleepTime);
      this.savedSleepTimeInLastInterval = toSleepTime;
      log.info(String.format("used-bw %f > bw-limit %f, force %d workers to sleep %d ms",
                                bwUsed, bwLimit, numWorkers, toSleepTime));
    } else {
      setSleepTimeInLastInterval(0L);
      setPerWorkerSleepTimeInLastInterval(0L);
      this.savedSleepTimeInLastInterval = 0;
    }
  }
}
