package de.claasklar.generation;

import de.claasklar.generation.inserters.FixedKeyObjectInserter;
import de.claasklar.generation.inserters.ObjectInserter;
import de.claasklar.generation.suppliers.ValueSupplier;
import de.claasklar.generation.suppliers.ValueSuppliers;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class ContextlessDocumentGeneratorBuilder {

  private final List<ObjectInserter> inserters;
  private final ValueSuppliers valueSuppliers;

  private ContextlessDocumentGeneratorBuilder() {
    this.inserters = new LinkedList<>();
    this.valueSuppliers = new ValueSuppliers(new StdRandomNumberGenerator());
  }

  public static ContextlessDocumentGeneratorBuilder builder() {
    return new ContextlessDocumentGeneratorBuilder();
  }

  /**
   * Convenience method for access to {@link ValueSuppliers}
   *
   * @see ContextDocumentGeneratorBuilder#field(String, ValueSupplier)
   * @param key key of the inserted field
   * @param config function, which returns the supplier for the field's value
   * @return this
   */
  public ContextlessDocumentGeneratorBuilder field(
      String key, Function<ValueSuppliers, ValueSupplier> config) {
    var supplier = config.apply(valueSuppliers);
    var inserter = new FixedKeyObjectInserter(key, supplier);
    inserters.add(inserter);
    return this;
  }

  /**
   * Apply the given ObjectInserter to the object. This is useful, when multiple fields are related
   * and need information from the same context. For example the effective price depends on the
   * applied tax and net price.
   *
   * @param inserter ObjectInserter is applied to object
   * @return this
   */
  public ContextlessDocumentGeneratorBuilder field(ObjectInserter inserter) {
    this.inserters.add(inserter);
    return this;
  }

  /**
   * Insert a field into the object with the given key and the value supplied by the {@link
   * ValueSupplier}
   *
   * @param key key of the inserted field
   * @param supplier supplies the field's value
   * @return this
   */
  public ContextlessDocumentGeneratorBuilder field(String key, ValueSupplier supplier) {
    inserters.add(new FixedKeyObjectInserter(key, supplier));
    return this;
  }

  public ContextlessDocumentGenerator build() {
    return new ContextlessDocumentGenerator(
        inserters.toArray(new ObjectInserter[inserters.size()]));
  }
}
