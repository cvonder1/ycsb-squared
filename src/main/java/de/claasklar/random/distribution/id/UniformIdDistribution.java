package de.claasklar.random.distribution.id;

import de.claasklar.random.distribution.RandomNumberGenerator;

public class UniformIdDistribution implements IdDistribution {

  private final long max;
  private final RandomNumberGenerator random;

  public UniformIdDistribution(long max, RandomNumberGenerator random) {
    if (max < 0) {
      throw new IllegalArgumentException("max must be greater zero");
    }
    this.max = max;
    this.random = random;
  }

  @Override
  public long nextAsLong() {
    return random.nextLong(0, max);
  }
}
