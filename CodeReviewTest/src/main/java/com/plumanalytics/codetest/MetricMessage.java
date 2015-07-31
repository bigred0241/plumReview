package com.plumanalytics.codetest;

import java.text.ParseException;

/**
 * Implemented by concrete message objects
 */
public interface MetricMessage {
  public void init(String line) throws ParseException;
}
