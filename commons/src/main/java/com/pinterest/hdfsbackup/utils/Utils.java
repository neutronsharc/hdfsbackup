package com.pinterest.hdfsbackup.utils;

import java.security.SecureRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

public class Utils
{
  public static String randomString(long value) {
    StringBuffer result = new StringBuffer();

    if (value < 0L)
      value = -value;
    do {
      long remainder = value % 58L;
      int c;
      if (remainder < 24L) {
        c = 'a' + (char)(int)remainder;
      }
      else if (remainder < 48L) {
        c = 'A' + (char)(int)(remainder - 24L);
      } else {
        c = '0' + (char) (int) (remainder - 48L);
      }
      result.appendCodePoint(c);
      value /= 58L;
    } while (value > 0L);
    return result.reverse().toString();
  }

  public static String randomString() {
    return randomString(new SecureRandom().nextLong());
  }

  public static String getSuffix(String name) {
    if (name != null) {
      String[] parts = name.split("\\.");
      if (parts.length > 1) {
        return parts[(parts.length - 1)];
      }
    }
    return "";
  }

  public static String replaceSuffix(String name, String suffix) {
    if (getSuffix(name).equals("")) {
      return name + suffix;
    }
    int index = name.lastIndexOf('.');
    return name.substring(0, index) + suffix;
  }

  public static ThreadPoolExecutor createDefaultExecutorService() {
    ThreadFactory threadFactory = new ThreadFactory() {
      private int threadCount = 1;

      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("hbasebackup-poolexec-thread-" + this.threadCount++);
        return thread;
      }
    };
    return (ThreadPoolExecutor) Executors.newFixedThreadPool(10, threadFactory);
  }

  public static String byteArrayToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }

}
