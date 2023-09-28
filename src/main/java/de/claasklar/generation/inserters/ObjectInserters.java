package de.claasklar.generation.inserters;

import de.claasklar.generation.suppliers.ValueSupplier;
import de.claasklar.generation.suppliers.ValueSuppliers;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import java.util.function.Function;

public class ObjectInserters {

  private static final StdRandomNumberGenerator stdRandomNumberGenerator =
      new StdRandomNumberGenerator();

  public ObjectInserter maybeField(
      String fieldName, double p, Function<ValueSuppliers, ValueSupplier> valueSupplierFactory) {
    if (p < 0 || p > 1) {
      throw new IllegalArgumentException("p must be in interval [0,1], p is " + p);
    }
    var valueSupplier = valueSupplierFactory.apply(new ValueSuppliers(stdRandomNumberGenerator));
    return objectValue -> {
      if (stdRandomNumberGenerator.nextDouble(0, 1) < p) {
        objectValue.put(fieldName, valueSupplier.get());
      }
    };
  }
}
