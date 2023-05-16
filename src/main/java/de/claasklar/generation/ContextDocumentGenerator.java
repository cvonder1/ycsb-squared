package de.claasklar.generation;

import de.claasklar.generation.inserters.Context;
import de.claasklar.generation.inserters.InserterFactory;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.ObjectValue;
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
  public Document generateDocument(Id id) {
    return this.generateDocument(id, Collections.emptyMap());
  }

  @Override
  public Document generateDocument(Id id, Map<CollectionName, Document[]> references) {
    var document = new Document(id, new HashMap<>());
    var context = new Context(references);
    Arrays.stream(inserters)
        .map(factory -> factory.apply(context))
        .forEach(inserter -> inserter.accept(document));
    return document;
  }
}
