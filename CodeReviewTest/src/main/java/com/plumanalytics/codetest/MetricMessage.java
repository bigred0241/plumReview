package com.plumanalytics.codetest;

import java.text.ParseException;

/**
 * Implemented by concrete message objects
 */
public interface MetricMessage {

  //expecting some API-level documentation here     
	//(same for all public methods that are not basic getters/setters)
  public void init(String line) throws ParseException;
}
