package de.claasklar.random.distribution.id;

import de.claasklar.random.distribution.DistributionProperties;
import de.claasklar.random.distribution.RandomNumberGenerator;
import java.util.List;

public class UniformIdDistribution implements IdDistribution {

  private final long max;
  private final RandomNumberGenerator random;

  public UniformIdDistribution(long max, RandomNumberGenerator random) {
    if (max <= 0) {
      throw new IllegalArgumentException("max must be greater zero");
    }
    this.max = max;
    this.random = random;
  }

  @Override
  public long nextAsLong() {
    return random.nextLong(0, max);
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return List.of(DistributionProperties.REPEATABLE);
  }
}
