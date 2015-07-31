package com.plumanalytics.codetest;

import java.text.ParseException;

/**
 * Publish Metrics
 */
public interface MetricPublisher {

  public MetricMessage createMessage(String line) throws ParseException;
  public void publishMetric(MetricMessage metricMessage);

}
