package de.claasklar.generation;

import de.claasklar.generation.inserters.FixedKeyObjectInserter;
import de.claasklar.generation.inserters.ObjectInserter;
import de.claasklar.generation.suppliers.ValueSupplier;
import de.claasklar.generation.suppliers.ValueSuppliers;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class ContextlessDocumentGeneratorBuilder {

  private final List<ObjectInserter> inserters;
  private final ValueSuppliers valueSuppliers;

  private ContextlessDocumentGeneratorBuilder() {
    this.inserters = new LinkedList<>();
    this.valueSuppliers = new ValueSuppliers();
  }

  public static ContextlessDocumentGeneratorBuilder builder() {
    return new ContextlessDocumentGeneratorBuilder();
  }

  public ContextlessDocumentGeneratorBuilder field(String key, ObjectInserter inserter) {
    this.inserters.add(inserter);
    return this;
  }

  public ContextlessDocumentGeneratorBuilder field(
      String key, Function<ValueSuppliers, ValueSupplier> config) {
    var supplier = config.apply(valueSuppliers);
    var inserter = new FixedKeyObjectInserter(key, supplier);
    inserters.add(inserter);
    return this;
  }

  public ContextlessDocumentGenerator build() {
    return new ContextlessDocumentGenerator(
        inserters.toArray(new ObjectInserter[inserters.size()]));
  }
}
