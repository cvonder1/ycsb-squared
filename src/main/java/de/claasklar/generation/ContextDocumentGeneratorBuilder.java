package de.claasklar.generation;

import de.claasklar.generation.inserters.InserterFactories;
import de.claasklar.generation.inserters.InserterFactory;
import de.claasklar.generation.inserters.ObjectInserter;
import de.claasklar.generation.inserters.ObjectInserters;
import de.claasklar.generation.pipes.Pipe;
import de.claasklar.generation.pipes.Pipes;
import de.claasklar.generation.pipes.Pipes.PipeBuilder;
import de.claasklar.generation.suppliers.ValueSupplier;
import de.claasklar.generation.suppliers.ValueSuppliers;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.ObjectValue;
import de.claasklar.primitives.document.OurDocument;
import de.claasklar.primitives.document.Value;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public class ContextDocumentGeneratorBuilder {

  private final List<InserterFactory<ObjectValue>> inserterFactories;
  private final InserterFactories factory = new InserterFactories();

  private ContextDocumentGeneratorBuilder() {
    this.inserterFactories = new LinkedList<>();
  }

  public static ContextDocumentGeneratorBuilder builder() {
    return new ContextDocumentGeneratorBuilder();
  }

  public ContextDocumentGenerator build() {
    return new ContextDocumentGenerator(
        inserterFactories.toArray(new InserterFactory[inserterFactories.size()]));
  }

  /**
   * Insert a field into the object with the given key and the value supplied by the {@link
   * ValueSupplier}
   *
   * @param key key of the inserted field
   * @param supplier supplies the field's value
   * @return this
   */
  public ContextDocumentGeneratorBuilder field(String key, ValueSupplier supplier) {
    this.inserterFactories.add(factory.insertFromSupplier(key, supplier));
    return this;
  }

  /**
   * Convenience method for access to {@link ValueSuppliers}
   *
   * @see ContextDocumentGeneratorBuilder#field(String, ValueSupplier)
   * @param key key of the inserted field
   * @param config function, which returns the supplier for the field's value
   * @return this
   */
  public ContextDocumentGeneratorBuilder field(
      String key, Function<ValueSuppliers, ValueSupplier> config) {
    return this.field(key, config.apply(new ValueSuppliers(new StdRandomNumberGenerator())));
  }

  /**
   * Inserts a value provided by the pipe into the object with the given key.
   *
   * @param key key of the inserted field
   * @param fieldConfig configure the pipe
   * @return this
   */
  public ContextDocumentGeneratorBuilder fieldFromPipe(
      String key, Consumer<FieldPipeBuilder> fieldConfig) {
    var fieldBuilder = new FieldPipeBuilder();
    fieldConfig.accept(fieldBuilder);
    this.inserterFactories.add(factory.insertFromPipe(key, fieldBuilder.build()));
    return this;
  }

  public <T, U> ContextDocumentGeneratorBuilder field(
      T initialState,
      Function<T, U> extract,
      Function<T, T> transform,
      Function<U, ObjectInserter> mapper) {
    ObjectInserter objectInserter =
        new ObjectInserter() {
          private T state = initialState;

          @Override
          public void accept(ObjectValue objectValue) {
            U nextState;
            synchronized (this) {
              nextState = extract.apply(state);
              state = transform.apply(state);
            }
            mapper.apply(nextState).accept(objectValue);
          }
        };
    this.inserterFactories.add(factory.insertFromObjectInserter(objectInserter));
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
  public ContextDocumentGeneratorBuilder fieldObjectInserter(ObjectInserter inserter) {
    this.inserterFactories.add(factory.insertFromObjectInserter(inserter));
    return this;
  }

  /**
   * Get ObjectInserter from {@link ObjectInserters} factory.
   *
   * @see ContextDocumentGeneratorBuilder#fieldObjectInserter(ObjectInserter)
   * @param inserterFactory create ObjectInserter used to populate the object
   * @return this
   */
  public ContextDocumentGeneratorBuilder fieldObjectInserters(
      Function<ObjectInserters, ObjectInserter> inserterFactory) {
    return this.fieldObjectInserter(inserterFactory.apply(new ObjectInserters()));
  }

  public static class FieldPipeBuilder {

    private Pipe<Map<CollectionName, OurDocument[]>, ? extends Value> pipe;

    /**
     * Select the collection with collectionName from all referenced collections. The output to the
     * next pipe is an array {@link OurDocument OurDocument[]}.
     *
     * @param collectionName collection name
     * @param pipeConfig further pipe configuration
     * @return Pipe, which returns a Value
     */
    public Pipe<Map<CollectionName, OurDocument[]>, ? extends Value> selectCollection(
        CollectionName collectionName,
        Function<PipeBuilder, Pipe<Map<CollectionName, OurDocument[]>, ? extends Value>>
            pipeConfig) {
      var pipeBuilder = Pipes.selectCollection(collectionName);
      this.pipe = pipeConfig.apply(pipeBuilder);
      return this.pipe;
    }

    public Pipe<Map<CollectionName, OurDocument[]>, ? extends Value> build() {
      return this.pipe;
    }
  }
}
