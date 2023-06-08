package de.claasklar.random.distribution;

import java.util.concurrent.ThreadLocalRandom;

public class StdRandomNumberGenerator implements RandomNumberGenerator {

  @Override
  public long nextLong(long min, long max) {
    return ThreadLocalRandom.current().nextLong(min, max);
  }

  @Override
  public int nextInt(int min, int max) {
    return ThreadLocalRandom.current().nextInt(min, max);
  }

  @Override
  public double nextDouble(double min, double max) {
    return ThreadLocalRandom.current().nextDouble(min, max);
  }
}
