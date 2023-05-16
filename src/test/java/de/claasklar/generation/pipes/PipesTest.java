package de.claasklar.generation.pipes;

import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.ReadContext;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.ArrayValue;
import de.claasklar.primitives.document.BoolValue;
import de.claasklar.primitives.document.Document;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.StringValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class PipesTest {

  @Test
  public void testReferencesSelectionPipe() {
    // given
    var pipe = Pipes.selectCollection(new CollectionName("test")).build();
    var document = new Document(randomId(), Map.of("hello", new StringValue("world")));
    // when
    var result =
        (ReadContext) pipe.apply(Map.of(new CollectionName("test"), new Document[] {document}));
    // then
    assertThat((Object) result.read("$.[0]._id")).isEqualTo(document.id().id());
    assertThat((Object) result.read("$.[0].hello")).isEqualTo("world");
  }

  @Test
  public void testSelectByPathShouldReturnSelectedFields() {
    // given
    var pipe =
        Pipes.selectCollection(new CollectionName("test")).selectByPath("$.[0,1]._id").build();
    var documents =
        new Document[] {
          new Document(randomId(), Collections.emptyMap()),
          new Document(randomId(), Collections.emptyMap()),
          new Document(randomId(), Collections.emptyMap())
        };
    var collections = Map.of(new CollectionName("test"), documents);
    // when
    var result = (List<byte[]>) pipe.apply(collections);
    // then
    assertThat(result).containsExactly(documents[0].id().id(), documents[1].id().id());
  }

  @Test
  public void testToArrayShouldReturnBooleanArray() {
    // given
    var pipe =
        Pipes.selectCollection(new CollectionName("test"))
            .selectByPath("$.[0,1].boolean")
            .toArray();
    var documents =
        new Document[] {
          new Document(randomId(), Map.of("boolean", new BoolValue(true))),
          new Document(randomId(), Map.of("boolean", new BoolValue(false))),
          new Document(randomId(), Map.of("boolean", new BoolValue(true)))
        };
    var collections = Map.of(new CollectionName("test"), documents);
    // when
    var result = (ArrayValue) pipe.apply(collections);
    // then
    assertThat(result).containsExactly(new BoolValue(true), new BoolValue(false));
  }

  private Id randomId() {
    var bytes = new byte[12];
    new Random().nextBytes(bytes);
    return new Id(bytes);
  }
}
