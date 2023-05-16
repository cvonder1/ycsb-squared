package de.claasklar;

import de.claasklar.database.InMemoryDatabase;
import de.claasklar.generation.ContextDocumentGeneratorBuilder;
import de.claasklar.generation.ContextlessDocumentGeneratorBuilder;
import de.claasklar.generation.suppliers.Suppliers;
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
    var documentGenerator =
        ContextlessDocumentGeneratorBuilder.builder()
            .field("number", (Suppliers s) -> s.uniformIntSupplier(0, 1000))
            .build();
    var writeSpec = new WriteSpecification(collectionName, documentGenerator, database, idStore);
    registry.register(writeSpec);

    var idDistribution = new UniformIdDistribution(5000, new StdRandomNumberGenerator());
    var documentDistribution =
        new SimpleDocumentDistribution(collectionName, idDistribution, idStore, database, registry);
    var existingDocumentDistribution =
        new ExistingDocumentDistribution(
            50, documentDistribution, database, bufferedThreadExecutor);
    var referencesDistribution =
        new ConstantNumberReferencesDistribution(5, existingDocumentDistribution);

    var primaryDocumentGenerator =
        ContextDocumentGeneratorBuilder.builder()
            .field("s1", (s) -> s.uniformLengthStringSupplier(5, 10))
            .fieldFromPipe(
                "secondary",
                p ->
                    p.selectCollection(
                        collectionName,
                        pipeBuilder -> pipeBuilder.selectByPath("$.[*]._id").toArray()))
            .build();
    var primaryWriteSpecification =
        new PrimaryWriteSpecification(
            new CollectionName("primary"),
            new ReferencesDistribution[] {referencesDistribution},
            primaryDocumentGenerator,
            database,
            threadExecutor);

    var ids = new HashSet<>();

    try {
      long start = System.currentTimeMillis();
      for (int i = 0; i < 10000; i++) {
        var runnable = primaryWriteSpecification.runnable();
        runnable.run();
        ids.add(runnable.getDocument().id());
      }
      System.out.println(System.currentTimeMillis() - start);
      System.out.println("Total num ids: " + ids.size());
    } finally {
      threadExecutor.shutdown();
      bufferedThreadExecutor.shutdown();
    }
  }
}
