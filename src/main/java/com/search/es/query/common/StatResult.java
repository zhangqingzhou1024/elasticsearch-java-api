package com.search.es.query.common;

import java.util.List;

public class StatResult
{
  private String key;
  private long count;
  private List<StatResult> subResult;

  public StatResult(String key, long count)
  {
    this.key = key;
    this.count = count;
  }

  public String getKey()
  {
    return this.key;
  }

  public void setKey(String key)
  {
    this.key = key;
  }

  public long getCount()
  {
    return this.count;
  }

  public void setCount(long count)
  {
    this.count = count;
  }

  public List<StatResult> getSubResult()
  {
    return this.subResult;
  }

  public void setSubResult(List<StatResult> subResult)
  {
    this.subResult = subResult;
  }
}