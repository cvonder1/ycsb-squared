package de.claasklar.random.distribution.id;

import de.claasklar.random.distribution.DistributionProperties;
import java.util.List;

public class OffsetIdDistribution implements IdDistribution {

  private final long offset;
  private final IdDistribution delegate;

  public OffsetIdDistribution(long offset, IdDistribution delegate) {
    if (offset < 0) {
      throw new IllegalArgumentException("can only use a positive offset for the ids");
    }
    this.offset = offset;
    this.delegate = delegate;
  }

  @Override
  public long nextAsLong() {
    return delegate.nextAsLong() + offset;
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return delegate.distributionProperties();
  }
}
