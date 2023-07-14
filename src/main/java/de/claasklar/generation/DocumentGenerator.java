package de.claasklar.generation;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.primitives.document.OurDocument;
import java.util.Map;

public interface DocumentGenerator {

  OurDocument generateDocument(IdLong id);

  OurDocument generateDocument(IdLong id, Map<CollectionName, OurDocument[]> references);
}
