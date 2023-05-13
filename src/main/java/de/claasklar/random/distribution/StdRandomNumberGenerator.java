package de.claasklar.random.distribution;

import java.util.Random;

public class StdRandomNumberGenerator implements RandomNumberGenerator {

  private static final Random random = new Random();

  @Override
  public long nextLong(long min, long max) {
    return random.nextLong(min, max);
  }
}
