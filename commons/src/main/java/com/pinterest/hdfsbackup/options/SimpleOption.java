package com.pinterest.hdfsbackup.options;


public class SimpleOption extends OptionBase implements Option {
  public boolean value;

  SimpleOption(String arg, String desc) {
    super(arg, desc);
    this.value = false;
  }

  public int matches(String[] arguments, int matchIndex) {
    String argument = arguments[matchIndex];
    if (argument.equals(this.arg)) {
      this.value = true;
      return matchIndex + 1;
    }
    return matchIndex;
  }

  public String helpLine() {
    return this.arg + "   -   " + this.desc;
  }

  public boolean defined() {
    return this.value;
  }
}