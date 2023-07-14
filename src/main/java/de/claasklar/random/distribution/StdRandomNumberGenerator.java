package de.claasklar.random.distribution;

import java.util.Random;

public class StdRandomNumberGenerator implements RandomNumberGenerator {

  private final ThreadLocal<Random> randomThreadLocal = ThreadLocal.withInitial(Random::new);

  @Override
  public long nextLong(long min, long max) {
    return randomThreadLocal.get().nextLong(min, max);
  }

  @Override
  public int nextInt(int min, int max) {
    return randomThreadLocal.get().nextInt(min, max);
  }

  @Override
  public double nextDouble(double min, double max) {
    return randomThreadLocal.get().nextDouble(min, max);
  }

  public void setSeed(long seed) {
    randomThreadLocal.set(new Random(seed));
  }

  public void reset() {
    randomThreadLocal.set(new Random());
  }
}
