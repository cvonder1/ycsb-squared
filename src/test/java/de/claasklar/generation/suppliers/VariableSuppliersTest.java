package de.claasklar.generation.suppliers;

import de.claasklar.idStore.IdStore;
import de.claasklar.idStore.InMemoryIdStore;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.IdLong;
import de.claasklar.random.distribution.id.IdDistribution;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VariableSuppliersTest {
    private final IdStore idStore = new InMemoryIdStore();
    private final CollectionName collectionName = new CollectionName("test");
    private final VariableSuppliers testSubject = new VariableSuppliers(idStore);

    @Test
    public void testExistingIdShouldReturnTheOnlyExistingId() {
        //given
        var idDistribution = mock(IdDistribution.class);
        when(idDistribution.next()).thenReturn(new IdLong(1));
        idStore.store(collectionName, new IdLong(1));
        var variableSupplier = testSubject.existingId("test", collectionName, idDistribution);
        //when
        var variables = variableSupplier.get();
        //then
        assertThat(variables.entrySet()).contains(entry("test", new IdLong(1).toId()));
    }

    @Test
    public void testExistingIdShouldReturnTheSecondId() {
        //given
        var idDistribution = mock(IdDistribution.class);
        when(idDistribution.next()).thenReturn(new IdLong(1));
        when(idDistribution.next()).thenReturn(new IdLong(2));
        idStore.store(collectionName, new IdLong(2));
        var variableSupplier = testSubject.existingId("test", collectionName, idDistribution);
        //when
        var variables = variableSupplier.get();
        //then
        assertThat(variables.entrySet()).contains(entry("test", new IdLong(2).toId()));
    }
}
