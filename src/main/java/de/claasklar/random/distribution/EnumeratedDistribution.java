package de.claasklar.random.distribution;

import de.claasklar.util.Pair;
import java.util.List;

public class EnumeratedDistribution<T> implements Distribution<T> {

  private final RandomNumberGenerator random = new StdRandomNumberGenerator();
  private final Double[] accumulatedWeights;
  private final Object[] values;
  private final double sum;

  public EnumeratedDistribution(List<Pair<Double, T>> pmf) {
    var acc =
        new Object() {
          double val = 0;
        };
    accumulatedWeights =
        pmf.stream()
            .map(Pair::first)
            .map(
                it -> {
                  acc.val += it;
                  return acc.val;
                })
            .toArray(Double[]::new);
    values = pmf.stream().map(Pair::second).toArray(Object[]::new);
    sum = pmf.stream().map(Pair::first).reduce(0d, Double::sum);
  }

  @Override
  public T sample() {
    var choice = random.nextDouble(0, sum);
    int i;
    for (i = 0; choice < accumulatedWeights[i]; i++)
      ;
    return (T) values[i];
  }
}
