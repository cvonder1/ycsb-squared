package de.claasklar.random.distribution;

import java.util.function.Supplier;

public class LongDistributionFactory {

  private static final StdRandomNumberGenerator stdRandomNumberGenerator =
      new StdRandomNumberGenerator();

  public Distribution<Long> constant(long constant) {
    return new ConstantDistribution(constant);
  }

  /**
   * Uniform from lower bound to upper bound(exclusive).
   *
   * @param lowerBound lower bound
   * @param upperBound upper bound
   * @return long distribution
   */
  public Distribution<Long> uniform(long lowerBound, long upperBound) {
    return new UniformLongDistribution(lowerBound, upperBound, stdRandomNumberGenerator);
  }

  /**
   * Geometric distribution with the given p counting the number failures until the first success.
   * E[X]=(1-p)/p Var[X]=(1-p)/(p^2)
   *
   * @see org.apache.commons.math3.distribution.GeometricDistribution
   * @param p success probability
   * @return long distribution
   */
  public Distribution<Long> geometric(double p) {
    return new GeometricLongDistribution(p, stdRandomNumberGenerator);
  }

  /**
   * Shift the other distribution by a fixed offset.
   *
   * @param shift number to add to other distribution
   * @param distributionSupplier factory for other distribution
   * @return long distribution
   */
  public Distribution<Long> shift(long shift, Supplier<Distribution<Long>> distributionSupplier) {
    return new ShiftDistribution(distributionSupplier.get(), shift);
  }

  /**
   * Returns zero with probability pZero, otherwise samples the other distribution.
   *
   * @param pZero probability for zero
   * @param distributionSupplier factory for other distribution
   * @return long distribution
   */
  public Distribution<Long> zeroOrElse(
      double pZero, Supplier<Distribution<Long>> distributionSupplier) {
    return new ZeroOrElseDistribution(pZero, distributionSupplier.get(), stdRandomNumberGenerator);
  }
}
