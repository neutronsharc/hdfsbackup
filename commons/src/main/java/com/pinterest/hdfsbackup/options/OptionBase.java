package com.pinterest.hdfsbackup.options;


public abstract class OptionBase implements Option
{
  protected String arg;
  protected String desc;

  public OptionBase(String arg, String desc) {
    this.arg = arg;
    this.desc = desc;
  }

  public void require() {
    if (!defined())
      throw new RuntimeException("expected argument " + this.arg);
  }
}