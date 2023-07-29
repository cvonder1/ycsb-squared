package de.claasklar.random.distribution.id;

import de.claasklar.random.distribution.DistributionProperties;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class UniqueIdDistribution implements IdDistribution {

  private final AtomicLong currentId;

  public UniqueIdDistribution() {
    this.currentId = new AtomicLong(1);
  }

  @Override
  public long nextAsLong() {
    return currentId.getAndIncrement();
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return Collections.emptyList();
  }
}
