hdfsbackup
==========

Tools to move backup data between HDFS and S3 with data integrity verification.

This project includes several tools:

- s3copy
copy data between HDFS and S3 with MD5 checksum verification.

- compare-dir
compare two directories (at HDFS or S3) to detect missing files / different file
sizes / mismatched contents.  MD5 checksum is used to detect mismatch.

- compare-file
compare two files (at HDFS or S3) with MD5 checksum verify.

- cleanup-multipart-uploads
Delete failure multipart-upload chunks at S3.

- cleanup-dir
Delete old files from a HDFS dir to reclaim HDFS space.