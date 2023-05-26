package de.claasklar.generation;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.OurDocument;
import java.util.Map;

public interface DocumentGenerator {

  OurDocument generateDocument(Id id);

  OurDocument generateDocument(Id id, Map<CollectionName, OurDocument[]> references);
}
