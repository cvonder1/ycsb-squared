package de.claasklar.generation;

import de.claasklar.generation.inserters.ObjectInserter;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.Id;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ContextlessDocumentGenerator implements DocumentGenerator {

  private final ObjectInserter[] inserters;

  public ContextlessDocumentGenerator(ObjectInserter[] inserters) {
    this.inserters = inserters;
  }

  @Override
  public Document generateDocument(Id id) {
    var document = new Document(id, new HashMap<>());
    Arrays.stream(inserters).forEach(inserter -> inserter.accept(document));
    return document;
  }

  @Override
  public Document generateDocument(Id id, Map<CollectionName, Document[]> references) {
    return this.generateDocument(id);
  }
}
