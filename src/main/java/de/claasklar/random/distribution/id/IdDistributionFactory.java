package de.claasklar.random.distribution.id;

import de.claasklar.random.distribution.StdRandomNumberGenerator;

public class IdDistributionFactory {
  public UniformIdDistribution uniform(long max) {
    return new UniformIdDistribution(max, new StdRandomNumberGenerator());
  }
}
