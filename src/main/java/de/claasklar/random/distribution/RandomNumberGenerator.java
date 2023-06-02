package de.claasklar.random.distribution;

public interface RandomNumberGenerator {
  int nextInt(int min, int max);

  long nextLong(long min, long max);
}
