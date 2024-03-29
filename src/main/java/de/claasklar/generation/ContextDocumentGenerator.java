package de.claasklar.generation;

import de.claasklar.generation.inserters.Context;
import de.claasklar.generation.inserters.InserterFactory;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.document.ObjectValue;
import de.claasklar.primitives.document.OurDocument;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ContextDocumentGenerator implements DocumentGenerator {

  private final InserterFactory<ObjectValue>[] inserters;

  public ContextDocumentGenerator(InserterFactory<ObjectValue>[] inserters) {
    this.inserters = inserters;
  }

  @Override
  public OurDocument generateDocument(IdLong id) {
    return this.generateDocument(id, Collections.emptyMap());
  }

  @Override
  public OurDocument generateDocument(IdLong id, Map<CollectionName, OurDocument[]> references) {
    var document = new OurDocument(id.toId(), new HashMap<>());
    var context = new Context(references);
    Arrays.stream(inserters)
        .map(factory -> factory.apply(context))
        .forEach(inserter -> inserter.accept(document));
    return document;
  }
}
