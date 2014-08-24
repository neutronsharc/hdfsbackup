package com.pinterest.hdfsbackup.options;

public abstract interface Option
{
  public abstract int matches(String[] paramArrayOfString, int paramInt);

  public abstract String helpLine();

  public abstract void require();

  public abstract boolean defined();
}