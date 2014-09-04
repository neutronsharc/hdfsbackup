package com.pinterest.hdfsbackup.utils;

/**
 * Created by shawn on 9/3/14.
 */
public class Pair<L, R> {
  public L getL() {
    return l;
  }

  protected L l;

  public R getR() {
    return r;
  }

  protected R r;

  public Pair(L l, R r) {
    this.l = l;
    this.r = r;
  }
}
