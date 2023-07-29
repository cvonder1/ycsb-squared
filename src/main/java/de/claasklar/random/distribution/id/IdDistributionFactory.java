package de.claasklar.random.distribution.id;

import de.claasklar.random.distribution.StdRandomNumberGenerator;
import java.util.function.Function;

public class IdDistributionFactory {
  public UniformIdDistribution uniform(long max) {
    return new UniformIdDistribution(max, new StdRandomNumberGenerator());
  }

  public UniqueIdDistribution unique() {
    return new UniqueIdDistribution();
  }

  public OffsetIdDistribution offset(
      long offset, Function<IdDistributionFactory, IdDistribution> distributionFactory) {
    return new OffsetIdDistribution(offset, distributionFactory.apply(this));
  }
}
