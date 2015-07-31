package com.plumanalytics.codetest;

import java.io.*;
import java.net.URL;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Process metric files using a thread pool
 */
public class MetricProcessor {

  private final File sourceDir;
  private final MetricPublisher publisher = new TestMetricPublisher();

  public static void main(String args[]) {
    try {
      URL url = MetricProcessor.class.getResource("/test-data");
      File testDataDir = new File(url.toURI());
      MetricProcessor processor = new MetricProcessor(testDataDir);
      processor.run();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  private MetricProcessor(File sourceDir) {
    this.sourceDir = sourceDir;

  }

  protected void processFilesMultiThreaded(List<File> fileList) throws InterruptedException {
    LinkedBlockingQueue<Runnable> workQueueu = new LinkedBlockingQueue<Runnable>(5);
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(10, 10, 5000L, TimeUnit.MILLISECONDS, workQueueu, new ThreadPoolExecutor.CallerRunsPolicy());
    for (File oneFile : fileList) {
      threadPool.execute(new ProcessFileThread(oneFile));
    }
    threadPool.shutdown();
    while (!threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
      System.out.println("Waiting for queue to complete. QueueSize=" + workQueueu.size() + " ActiveThreads=" + threadPool.getActiveCount());
    }
    System.out.println("File list processing complete.");
  }

  protected List<File> listFiles() {
    return Arrays.asList(this.sourceDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return pathname.getName().endsWith(".txt");
        }
      }));
  }

  public void run() throws InterruptedException {
    processFilesMultiThreaded(listFiles());
    System.out.println(this.publisher);
  }

  private class ProcessFileThread implements Runnable {
    File sourceFile;

    ProcessFileThread(File sourceFile) {
      this.sourceFile = sourceFile;
    }

    @Override
    public void run() {
      System.out.println(Thread.currentThread().getId()+ " - Processing file: " + sourceFile.getName());
      try (BufferedReader reader = new BufferedReader(new FileReader(sourceFile))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          try {
            MetricMessage message = publisher.createMessage(line);
            publisher.publishMetric(message);
          //probably don't want to catch throwable here - according to the APIs used above, ParseException is the only exception we exepct to see
          } catch (ParseException e) {
            throw new RuntimeException("Unable to parse date from row in file: " + sourceFile.getAbsolutePath() + " - " + line, e);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to process file: " + sourceFile.getAbsolutePath(), e);
      }
    }
  }

}
