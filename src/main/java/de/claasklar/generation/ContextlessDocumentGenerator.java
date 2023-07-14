package de.claasklar.generation;

import de.claasklar.generation.inserters.ObjectInserter;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.document.OurDocument;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ContextlessDocumentGenerator implements DocumentGenerator {

  private final ObjectInserter[] inserters;

  public ContextlessDocumentGenerator(ObjectInserter[] inserters) {
    this.inserters = inserters;
  }

  @Override
  public OurDocument generateDocument(IdLong id) {
    var document = new OurDocument(id.toId(), new HashMap<>());
    Arrays.stream(inserters).forEach(inserter -> inserter.accept(document));
    return document;
  }

  @Override
  public OurDocument generateDocument(IdLong id, Map<CollectionName, OurDocument[]> references) {
    return this.generateDocument(id);
  }
}
