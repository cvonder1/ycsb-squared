package de.claasklar.random.distribution;

import java.util.List;

public class ConstantDistribution implements Distribution<Long> {

  private final long constant;

  public ConstantDistribution(long constant) {
    this.constant = constant;
  }

  @Override
  public Long sample() {
    return constant;
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return List.of(DistributionProperties.REPEATABLE);
  }
}
