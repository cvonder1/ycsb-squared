package de.claasklar.generation;

import de.claasklar.generation.inserters.FixedKeyObjectInserter;
import de.claasklar.generation.inserters.ObjectInserter;
import de.claasklar.generation.suppliers.Suppliers;
import de.claasklar.generation.suppliers.ValueSupplier;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class ContextlessDocumentGeneratorBuilder {

  private final List<ObjectInserter> inserters;
  private final Suppliers suppliers;

  private ContextlessDocumentGeneratorBuilder() {
    this.inserters = new LinkedList<>();
    this.suppliers = new Suppliers();
  }

  public static ContextlessDocumentGeneratorBuilder builder() {
    return new ContextlessDocumentGeneratorBuilder();
  }

  public ContextlessDocumentGeneratorBuilder field(String key, ObjectInserter inserter) {
    this.inserters.add(inserter);
    return this;
  }

  public ContextlessDocumentGeneratorBuilder field(
      String key, Function<Suppliers, ValueSupplier> config) {
    var supplier = config.apply(suppliers);
    var inserter = new FixedKeyObjectInserter(key, supplier);
    inserters.add(inserter);
    return this;
  }

  public ContextlessDocumentGenerator build() {
    return new ContextlessDocumentGenerator(
        inserters.toArray(new ObjectInserter[inserters.size()]));
  }
}
