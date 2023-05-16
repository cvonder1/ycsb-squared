package de.claasklar.generation.inserters;

import de.claasklar.generation.pipes.Pipe;
import de.claasklar.generation.suppliers.ValueSupplier;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.ObjectValue;
import de.claasklar.primitives.document.Value;
import java.util.Map;

public class InserterFactories {

  public InserterFactory<ObjectValue> insertFromPipe(
      String key, Pipe<Map<CollectionName, Document[]>, ? extends Value> pipe) {
    return (context) ->
        new FixedKeyObjectInserter(key, () -> pipe.curry(context.references()).get());
  }

  public InserterFactory<ObjectValue> insertFromSupplier(String key, ValueSupplier valueSupplier) {
    return context -> new FixedKeyObjectInserter(key, valueSupplier);
  }
}
