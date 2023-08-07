package de.claasklar.generation.suppliers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import de.claasklar.idStore.IdStore;
import de.claasklar.idStore.InMemoryIdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.random.distribution.id.IdDistributionFactory;
import org.junit.jupiter.api.Test;

public class VariableSuppliersTest {
  private final IdStore idStore = new InMemoryIdStore();
  private final CollectionName collectionName = new CollectionName("test");
  private final VariableSuppliers testSubject = new VariableSuppliers(idStore);
  private final IdDistributionFactory idDistributionFactory = new IdDistributionFactory();

  @Test
  public void testExistingIdShouldReturnTheOnlyExistingId() {
    // given
    idStore.store(collectionName, new IdLong(1));
    var variableSupplier =
        testSubject.existingId("test", collectionName, idDistributionFactory.uniform(4));
    // when
    var variables = variableSupplier.get();
    // then
    assertThat(variables.entrySet()).contains(entry("test", new IdLong(1).toId()));
  }

  @Test
  public void testExistingIdShouldReturnTheSecondId() {
    // given
    idStore.store(collectionName, new IdLong(2));
    var variableSupplier =
        testSubject.existingId("test", collectionName, idDistributionFactory.uniform(4));
    // when
    var variables = variableSupplier.get();
    // then
    assertThat(variables.entrySet()).contains(entry("test", new IdLong(2).toId()));
  }
}
