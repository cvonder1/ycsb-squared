package de.claasklar.random.distribution;

import java.util.List;

public class UniformDistribution<T> implements Distribution<T> {
  private final RandomNumberGenerator random = new StdRandomNumberGenerator();
  private final Object[] values;

  public UniformDistribution(T[] values) {
    this.values = values;
  }

  @Override
  public T sample() {
    return (T) values[random.nextInt(0, values.length)];
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return List.of(DistributionProperties.REPEATABLE);
  }
}
