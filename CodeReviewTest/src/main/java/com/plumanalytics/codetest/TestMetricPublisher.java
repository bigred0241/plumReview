package com.plumanalytics.codetest;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Used to publish metrics from test files
 */
public class TestMetricPublisher implements MetricPublisher {

  Map<Date, Map<String, CountInstance>> metricMapByDate = new HashMap<Date, Map<String, CountInstance>>();
  Map<String, CountInstance> aggregateCountMapById = new HashMap<String, CountInstance>();

  @Override
  public void publishMetric(MetricMessage message) {
    TestMetricMessage metricMessage = (TestMetricMessage) message;
    Map<String, CountInstance> oneMetric = metricMapByDate.get(metricMessage.getMetricDate());
    if (oneMetric == null) {
      oneMetric = new HashMap<String, CountInstance>();
      metricMapByDate.put(metricMessage.getMetricDate(), oneMetric);
    }
    CountInstance countInstance = oneMetric.get(metricMessage.getId());
    if (countInstance == null) {
      countInstance = new CountInstance();
      oneMetric.put(metricMessage.getId(), countInstance);
    }
    countInstance.addCounts(metricMessage.getCount1(), metricMessage.getCount2(), metricMessage.getCount3());

    CountInstance aggrCountInstance = aggregateCountMapById.get(metricMessage.getId());
    if (aggrCountInstance == null) {
      aggrCountInstance = new CountInstance();
      aggregateCountMapById.put(metricMessage.getId(), aggrCountInstance);
    }
    aggrCountInstance.addCounts(metricMessage.getCount1(), metricMessage.getCount2(), metricMessage.getCount3());
  }

  public MetricMessage createMessage(String line) throws ParseException {
    MetricMessage message = new TestMetricMessage();
    message.init(line);
    return message;
  }

  public String toString() {
    StringBuilder asString = new StringBuilder();
    asString.append(metricMapByDate.toString());
    asString.append("\n");
    asString.append(aggregateCountMapById.toString());
    return asString.toString();
  }

  class CountInstance {
    int count1;
    int count2;
    int count3;

    void addCounts(int count1, int count2, int count3) {
      this.count1 += count1;
      this.count2 += count2;
      this.count3 += count3;
    }
    public String toString() {
      return count1 + "\t" + count2 + "\t" + count3;
    }
  }

}
