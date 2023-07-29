package de.claasklar.random.distribution;

import java.util.List;

public class UniformLongDistribution implements Distribution<Long> {

  private final long lowerBound;
  private final long upperBound;
  private final StdRandomNumberGenerator stdRandomNumberGenerator;

  public UniformLongDistribution(
      long lowerBound, long upperBound, StdRandomNumberGenerator stdRandomNumberGenerator) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.stdRandomNumberGenerator = stdRandomNumberGenerator;
  }

  @Override
  public Long sample() {
    return stdRandomNumberGenerator.nextLong(lowerBound, upperBound);
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return List.of(DistributionProperties.REPEATABLE);
  }
}
