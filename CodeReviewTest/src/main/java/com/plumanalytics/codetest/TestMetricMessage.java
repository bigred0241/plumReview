package com.plumanalytics.codetest;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Concrete test message
 */
public class TestMetricMessage implements MetricMessage {
  private static SimpleDateFormat METRIC_DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd");

  private String id;
  private Date metricDate;
  private int count1;
  private int count2;
  private int count3;

  @Override
  public void init(String line) throws ParseException {
    String [] split = line.split("\\t");
    metricDate = METRIC_DATE_FORMAT.parse(split[0]);
    id = split[1];
    count1 = Integer.parseInt(split[2]);
    count2 = Integer.parseInt(split[3]);
    count3 = Integer.parseInt(split[4]);
  }

  public String getId() {
    return id;
  }

  public Date getMetricDate() {
    return metricDate;
  }

  public int getCount1() {
    return count1;
  }

  public int getCount2() {
    return count2;
  }

  public int getCount3() {
    return count3;
  }
}
