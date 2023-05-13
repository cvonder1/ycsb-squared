package de.claasklar;

import de.claasklar.database.InMemoryDatabase;
import de.claasklar.idStore.FileIdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import de.claasklar.random.distribution.document.ExistingDocumentDistribution;
import de.claasklar.random.distribution.document.SimpleDocumentDistribution;
import de.claasklar.random.distribution.id.UniformIdDistribution;
import de.claasklar.random.distribution.reference.ConstantNumberReferencesDistribution;
import de.claasklar.random.distribution.reference.ReferencesDistribution;
import de.claasklar.specification.PrimaryWriteSpecification;
import de.claasklar.specification.WriteSpecification;
import de.claasklar.specification.WriteSpecificationRegistry;
import java.util.HashSet;
import java.util.concurrent.Executors;

public class Main {

  public static void main(String[] args) {
    System.out.println("Hello world!");
    var idStore = new FileIdStore();
    var database = new InMemoryDatabase();
    var collectionName = new CollectionName("test_collection");
    var threadExecutor = Executors.newFixedThreadPool(5);
    var bufferedThreadExecutor = Executors.newFixedThreadPool(20);

    var registry = new WriteSpecificationRegistry();
    var writeSpec = new WriteSpecification(collectionName, database, idStore);
    registry.register(writeSpec);

    var idDistribution = new UniformIdDistribution(200, new StdRandomNumberGenerator());
    var documentDistribution =
        new SimpleDocumentDistribution(collectionName, idDistribution, idStore, database, registry);
    var existingDocumentDistribution =
        new ExistingDocumentDistribution(
            50, documentDistribution, database, bufferedThreadExecutor);
    var referencesDistribution =
        new ConstantNumberReferencesDistribution(5, existingDocumentDistribution);

    var primaryWriteSpecification =
        new PrimaryWriteSpecification(
            new CollectionName("primary"),
            new ReferencesDistribution[] {referencesDistribution},
            database,
            threadExecutor);

    var ids = new HashSet<>();

    try {
      for (int i = 0; i < 200; i++) {
        var runnable = primaryWriteSpecification.runnable();
        runnable.run();
        System.out.println(runnable.getDocument().id());
        ids.add(runnable.getDocument().id());
      }
      System.out.println("Total num ids: " + ids.size());
    } finally {
      threadExecutor.shutdown();
      bufferedThreadExecutor.shutdown();
    }
  }
}
