package de.claasklar.random.distribution;

import java.util.List;
import org.apache.commons.math3.distribution.GeometricDistribution;
import org.apache.commons.math3.random.RandomGenerator;

/** Geometric distribution with PMF p(1-p)^k and k in {0,1,2,3,...} */
public class GeometricLongDistribution implements Distribution<Long> {

  private final GeometricDistribution geometricDistribution;

  public GeometricLongDistribution(double p, StdRandomNumberGenerator stdRandomNumberGenerator) {
    var randomGenerator =
        new RandomGenerator() {
          @Override
          public void setSeed(int seed) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void setSeed(int[] seed) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void setSeed(long seed) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void nextBytes(byte[] bytes) {
            throw new UnsupportedOperationException();
          }

          @Override
          public int nextInt() {
            throw new UnsupportedOperationException();
          }

          @Override
          public int nextInt(int n) {
            throw new UnsupportedOperationException();
          }

          @Override
          public long nextLong() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean nextBoolean() {
            throw new UnsupportedOperationException();
          }

          @Override
          public float nextFloat() {
            throw new UnsupportedOperationException();
          }

          @Override
          public double nextDouble() {
            return stdRandomNumberGenerator.nextDouble(0, 1);
          }

          @Override
          public double nextGaussian() {
            throw new UnsupportedOperationException();
          }
        };
    this.geometricDistribution = new GeometricDistribution(randomGenerator, p);
  }

  @Override
  public Long sample() {
    return (long) geometricDistribution.sample();
  }

  @Override
  public List<DistributionProperties> distributionProperties() {
    return List.of(DistributionProperties.REPEATABLE);
  }
}
