package org.ecommerce.analyticsservice.jobs;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ScheduleTasks {
  private static final Logger logger =
      LoggerFactory.getLogger(ScheduleTasks.class);

  private static final long scheduleTime = 1800000;

  private final OrderAnalyticsJob orderAnalyticsJob;
  private final ProductAnalyticsJob productAnalyticsJob;
  private final CategoryAnalyticsJob categoryAnalyticsJob;
  private final TagAnalyticsJob tagAnalyticsJob;
  private final CustomerAnalyticsJob customerAnalyticsJob;

  @Scheduled(fixedDelay = scheduleTime)
  public void performOrderAnalyticsTasks() {
    long t0 = System.currentTimeMillis();
    logger.debug("START Order Analytics Job: ");
    orderAnalyticsJob.start();
    logger.debug("END Order Analytics Job: {}ms",
                 System.currentTimeMillis() - t0);
  }

  @Scheduled(fixedDelay = scheduleTime)
  public void performProductAnalyticsTasks() {
    long t0 = System.currentTimeMillis();
    logger.debug("START Product Analytics Job: ");
    productAnalyticsJob.start();
    logger.debug("END Product Analytics Job: {}ms",
                 System.currentTimeMillis() - t0);
  }

  @Scheduled(fixedDelay = scheduleTime)
  public void performCategoryAnalyticsTasks() {
    long t0 = System.currentTimeMillis();
    logger.debug("START Category Analytics Job:");
    categoryAnalyticsJob.start();
    logger.debug("END Category Analytics Job: {}ms",
                 System.currentTimeMillis() - t0);
  }

  @Scheduled(fixedDelay = scheduleTime)
  public void performTagAnalyticsTasks() {
    long t0 = System.currentTimeMillis();
    logger.debug("START Tag Analytics Job:");
    tagAnalyticsJob.start();
    logger.debug("END Tag Analytics Job: {}ms",
                 System.currentTimeMillis() - t0);
  }

  @Scheduled(fixedDelay = scheduleTime)
  public void performCustomerAnalyticsTasks() {
    long t0 = System.currentTimeMillis();
    logger.debug("START Customer Analytics Job:");
    customerAnalyticsJob.start();
    logger.debug("END Customer Analytics Job: {}ms",
                 System.currentTimeMillis() - t0);
  }
}
