package de.claasklar.generation.inserters;

import de.claasklar.generation.suppliers.ValueSupplier;
import de.claasklar.primitives.document.ObjectValue;

public class FixedKeyObjectInserter implements ObjectInserter {

  private final String key;
  private final ValueSupplier valueSupplier;

  public FixedKeyObjectInserter(String key, ValueSupplier valueSupplier) {
    this.key = key;
    this.valueSupplier = valueSupplier;
  }

  @Override
  public void accept(ObjectValue objectValue) {
    objectValue.put(key, valueSupplier.get());
  }
}
