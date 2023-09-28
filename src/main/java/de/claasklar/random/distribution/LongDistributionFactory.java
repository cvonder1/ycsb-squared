package de.claasklar.random.distribution;

import java.util.function.Supplier;

public class LongDistributionFactory {

  private static final StdRandomNumberGenerator stdRandomNumberGenerator =
      new StdRandomNumberGenerator();

  public Distribution<Long> constant(long constant) {
    return new ConstantDistribution(constant);
  }

  public Distribution<Long> uniform(long lowerBound, long upperBound) {
    return new UniformLongDistribution(lowerBound, upperBound, stdRandomNumberGenerator);
  }

  public Distribution<Long> geometric(double p) {
    return new GeometricLongDistribution(p, stdRandomNumberGenerator);
  }

  public Distribution<Long> shift(long shift, Supplier<Distribution<Long>> distributionSupplier) {
    return new ShiftDistribution(distributionSupplier.get(), shift);
  }

  public Distribution<Long> zeroOrElse(
      double pZero, Supplier<Distribution<Long>> distributionSupplier) {
    return new ZeroOrElseDistribution(pZero, distributionSupplier.get(), stdRandomNumberGenerator);
  }
}
