package de.claasklar.generation.pipes;

import static org.assertj.core.api.Assertions.assertThat;

import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class PipesTest {

  @Test
  public void testSelectByPathShouldReturnSelectedFields() {
    // given
    var pipe =
        Pipes.selectCollection(new CollectionName("test")).selectByPath("$.[0,1]._id").build();
    var documents =
        new OurDocument[] {
          new OurDocument(randomId(), Collections.emptyMap()),
          new OurDocument(randomId(), Collections.emptyMap()),
          new OurDocument(randomId(), Collections.emptyMap())
        };
    var collections = Map.of(new CollectionName("test"), documents);
    // when
    var result = (List<byte[]>) pipe.apply(collections);
    // then
    assertThat(result).containsExactly(documents[0].getId().id(), documents[1].getId().id());
  }

  @Test
  public void testToArrayShouldReturnBooleanArray() {
    // given
    var pipe =
        Pipes.selectCollection(new CollectionName("test"))
            .selectByPath("$.[0,1].boolean")
            .toArray();
    var documents =
        new OurDocument[] {
          new OurDocument(randomId(), Map.of("boolean", new BoolValue(true))),
          new OurDocument(randomId(), Map.of("boolean", new BoolValue(false))),
          new OurDocument(randomId(), Map.of("boolean", new BoolValue(true)))
        };
    var collections = Map.of(new CollectionName("test"), documents);
    // when
    var result = (ArrayValue) pipe.apply(collections);
    // then
    assertThat(result).containsExactly(new BoolValue(true), new BoolValue(false));
  }

  @Test
  public void testToObjectShouldExtractObjectFromArray() {
    // given
    var pipe = Pipes.selectCollection(new CollectionName("test")).selectByPath("$.[0]").toObject();
    var documents =
        new OurDocument[] {
          new OurDocument(randomId(), Map.of("boolean", new BoolValue(true))),
        };
    var collections = Map.of(new CollectionName("test"), documents);
    // when
    var result = pipe.apply(collections);
    // then
    assertThat(result)
        .isEqualTo(
            new NestedObjectValue(
                Map.of("_id", documents[0].getId(), "boolean", new BoolValue(true))));
  }

  private Id randomId() {
    var bytes = new byte[12];
    new Random().nextBytes(bytes);
    return new Id(bytes);
  }
}
