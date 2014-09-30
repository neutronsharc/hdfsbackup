package com.pinterest.hdfsbackup.s3copy;

import com.pinterest.hdfsbackup.s3tools.S3CopyOptions;
import com.pinterest.hdfsbackup.s3tools.S3Downloader;
import com.pinterest.hdfsbackup.s3tools.S3Uploader;
import com.pinterest.hdfsbackup.utils.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;

import java.util.Map;
import java.util.UUID;

/**
 * Created by shawn on 8/26/14.
 */
public class S3Copy extends Configured implements Tool {
  private static final Log log = LogFactory.getLog(S3Copy.class);
  private Configuration conf;


  public int copyOneFile(DirEntry srcEntry, S3CopyOptions options) {
    FSType srcType = FileUtils.getFSType(options.srcPath);
    S3Downloader s3Downloader = null;
    S3Uploader s3Uploader = null;

    if (!srcEntry.isFile) {
      log.info("source object is a dir: " + srcEntry.toString());
      return 0;
    }
    try {
      Progressable progressable = new Progressable() {
        public void progress() {
        }
      };
      if (srcType == FSType.S3) {
        // Copy from S3 to HDFs.
        s3Downloader = new S3Downloader(this.conf, options, progressable);
        if (s3Downloader.DownloadFile(srcEntry, options.destPath, options.verifyChecksum)) {
          return 0;
        } else {
          return 1;
        }
      } else {
        // copy from HDFS to S3.  Must specify dest S3 dir.
        if (options.destPath == null) {
          log.info("Please provide dest directory.");
          return 1;
        }
        s3Uploader = new S3Uploader(this.conf, options, progressable);
        if (s3Uploader.uploadFile(srcEntry, options.destPath)) {
          return 0;
        } else {
          return 1;
        }
      }
    } finally {
      if (s3Downloader != null) s3Downloader.close();
      if (s3Uploader != null) s3Uploader.close();
    }
  }

  @Override
  public int run(String[] strings) throws Exception {
    S3CopyOptions options = new S3CopyOptions(strings);
    if (options.helpDefined) {
      return 0;
    }
    options.populateFromConfiguration(this.conf);
    options.showCopyOptions();

    // NOTE: There is a special case:  src is S3, and no dest specified. We will download
    // the S3 objects, verify the actual content checksum matches with the checksum stored
    // in the object metadata. The S3 object is not saved anywhere.
    // This mode is useful for integrity check of S3 objects.

    FilePairGroup filePairGroup = null;
    FileListingInDir srcFileList = null;
    FSType srcType;
    FSType destType;
    boolean toS3 = false;

    if (options.manifestFilename != null) {
      filePairGroup = new FilePairGroup(0);

      long count = filePairGroup.initFromFile(options.manifestFilename);
      if (count <= 0) {
        log.info("Error parsing manifest file: " + options.manifestFilename);
        return 1;
      } else {
        log.info(String.format("Will copy %d files at manifest file: %s",
                                  count, options.manifestFilename));
      }
      srcType = FileUtils.getFSType(filePairGroup.getFilePairs().get(0).srcFile.toString());
      destType = FileUtils.getFSType(filePairGroup.getFilePairs().get(0).destFile.toString());

    } else {
      DirWalker dirWalker = new DirWalker(this.conf);
      //FileListingInDir srcFileList = dirWalker.walkDir(options.srcPath);
      srcFileList = dirWalker.walkDir(options.srcPath);
      if (srcFileList == null) {
        log.info("Error: source dir non-exist: " + options.srcPath);
        return 1;
      }
      // A special case: only download / upload one file between HDFS and S3.
      if (srcFileList.getFileEntryCount() == 1) {
        for (Map.Entry<String, DirEntry> e : srcFileList.getFileEntries()) {
          log.info("will copy only one file: " + e.getValue().toString());
          return copyOneFile(e.getValue(), options);
        }
      }
      srcType = FileUtils.getFSType(options.srcPath);
      destType = FileUtils.getFSType(options.destPath);
    }
    // NOTE: if src is S3 and dest is null, we can still download the S3 files and verify its
    // content against the md5 checksum in s3 obj metadata.  The downloaded objs are
    // not really saved anywhere though.
    // TODO(shawn@): support local file.
    if (srcType == FSType.S3 && destType == FSType.HDFS) {
      log.info("from S3 to HDFS");
      toS3 = false;
    } else if (srcType == FSType.S3 && destType == FSType.UNKNOWN) {
      log.info("Scan S3 for integrity check");
      toS3 = false;
    } else if (srcType == FSType.HDFS && destType == FSType.S3) {
      log.info("from HDFS to S3");
      toS3 = true;
    } else {
      log.info("unsupported transmission");
      return 1;
    }

    // need to copy from src to dest dir.
    // We will overwrite all contents in dest dir.
    String tempDirRoot = "hdfs:///tmp/" + UUID.randomUUID();
    FileUtils.createHDFSDir(tempDirRoot, this.conf);
    Path mapInputDirPath = new Path(tempDirRoot, "map-input");
    Path redOutputDirPath = new Path(tempDirRoot, "red-output");
    log.info("Use tmp dir: " + tempDirRoot);

    try {
      JobConf job = new JobConf(getConf(), S3Copy.class);

      // Each mapper takes care of a file group. We don't need reducers.
      job.setNumReduceTasks(0);
      int numberMappers = job.getNumMapTasks();
      FilePairPartition partition = new FilePairPartition(numberMappers);
      if (srcFileList != null) {
        partition.createFileGroups(srcFileList, options.destPath);
        job.setJobName(String.format("S3Copy  %s => %s,  %s checksum",
                                        options.srcPath, options.destPath,
                                        options.verifyChecksum ? "with" : "no"));
      } else {
        partition.createFileGroupsFromFilePairs(filePairGroup.getFilePairs());
        job.setJobName(String.format("S3Copy  mainfest = %s,  %s checksum",
                                        options.manifestFilename,
                                        options.verifyChecksum ? "with" : "no"));
      }
      partition.display(options.verbose);
      if (!partition.writeGroupsToFiles(mapInputDirPath, this.conf)) {
        log.info("failed to write file group files.");
        return 1;
      }

      // set up options
      job.setInputFormat(SequenceFileInputFormat.class);
      job.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, mapInputDirPath);
      FileOutputFormat.setOutputPath(job, redOutputDirPath);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(FilePair.class);

      if (toS3) {
        job.setMapperClass(S3PutMapper.class);
      } else {
        job.setMapperClass(S3GetMapper.class);
      }
      //job.setReducerClass(S3GetReducer.class);

      log.info("before MR job...");
      RunningJob runningJob = JobClient.runJob(job);
      log.info("after MR job...");
      Counters counters = runningJob.getCounters();
      Counters.Group group = counters.getGroup("org.apache.hadoop.mapreduce.TaskCounter");
      long outputRecords = group.getCounterForName("MAP_OUTPUT_RECORDS").getValue();
      log.info("MR job finished, " + outputRecords + " files failed to download");
      int retcode = (int) outputRecords;
      return retcode;
    }
    finally {
      FileUtils.deleteHDFSDir(tempDirRoot, this.conf);
    }
  }

  @Override
  public void setConf(Configuration entries) {
    this.conf = entries;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }
}
