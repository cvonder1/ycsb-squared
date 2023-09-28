package de.claasklar.random.distribution;

import java.util.List;

public class ZeroOrElseDistribution implements Distribution<Long> {

  private final double pZero;
  private final Distribution<Long> otherDistribution;
  private final StdRandomNumberGenerator stdRandomNumberGenerator;

  public ZeroOrElseDistribution(
      double pZero,
      Distribution<Long> otherDistribution,
      StdRandomNumberGenerator stdRandomNumberGenerator) {
    this.pZero = pZero;
    this.otherDistribution = otherDistribution;
    this.stdRandomNumberGenerator = stdRandomNumberGenerator;
  }

  @Override
  public Long sample() {
    if (stdRandomNumberGenerator.nextDouble(0, 1) <= pZero) {
      return 0L;
    } else {
      return otherDistribution.sample();
    }
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return DistributionProperties.and(
        otherDistribution.distributionProperties(), List.of(DistributionProperties.REPEATABLE));
  }
}
