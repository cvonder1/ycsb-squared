package de.claasklar.random.distribution.id;

import de.claasklar.random.distribution.StdRandomNumberGenerator;
import java.util.function.Function;

public class IdDistributionFactory {

  /**
   * Create IdDistribution that selects ids uniformly from 1 to max.
   *
   * @param max upper id bound
   * @return id distribution
   */
  public UniformIdDistribution uniform(long max) {
    return new UniformIdDistribution(max, new StdRandomNumberGenerator());
  }

  /**
   * Create IdDistribution that returns a new id each time it is invoked.
   *
   * @return unique id distribution
   */
  public UniqueIdDistribution unique() {
    return new UniqueIdDistribution();
  }

  /**
   * Offset another distribution by a fixed number.
   *
   * @param offset offset to apply
   * @param distributionFactory factory for other distribtuion
   * @return id distribution
   */
  public OffsetIdDistribution offset(
      long offset, Function<IdDistributionFactory, IdDistribution> distributionFactory) {
    return new OffsetIdDistribution(offset, distributionFactory.apply(this));
  }
}
