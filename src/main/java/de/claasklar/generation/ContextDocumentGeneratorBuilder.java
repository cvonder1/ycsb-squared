package de.claasklar.generation;

import de.claasklar.generation.inserters.InserterFactories;
import de.claasklar.generation.inserters.InserterFactory;
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

  public ContextDocumentGeneratorBuilder field(String key, ValueSupplier supplier) {
    this.inserterFactories.add(factory.insertFromSupplier(key, supplier));
    return this;
  }

  public ContextDocumentGeneratorBuilder field(
      String key, Function<ValueSuppliers, ValueSupplier> config) {
    return this.field(key, config.apply(new ValueSuppliers(new StdRandomNumberGenerator())));
  }

  public ContextDocumentGeneratorBuilder fieldFromPipe(
      String key, Consumer<FieldPipeBuilder> fieldConfig) {
    var fieldBuilder = new FieldPipeBuilder();
    fieldConfig.accept(fieldBuilder);
    this.inserterFactories.add(factory.insertFromPipe(key, fieldBuilder.build()));
    return this;
  }

  public static class FieldPipeBuilder {

    private Pipe<Map<CollectionName, OurDocument[]>, ? extends Value> pipe;

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
