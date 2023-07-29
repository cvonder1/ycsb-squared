package de.claasklar.random.distribution;

import java.util.List;

public class ShiftDistribution implements Distribution<Long> {

  private final Distribution<Long> sourceDistribution;
  private final long shift;

  public ShiftDistribution(Distribution<Long> sourceDistribution, long shift) {
    this.sourceDistribution = sourceDistribution;
    if (shift < 0) {
      throw new IllegalArgumentException("cannot shift distribution to the left");
    }
    this.shift = shift;
  }

  @Override
  public Long sample() {
    return sourceDistribution.sample() + shift;
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return List.of(DistributionProperties.REPEATABLE);
  }
}
