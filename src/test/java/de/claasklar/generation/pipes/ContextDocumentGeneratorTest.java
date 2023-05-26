package de.claasklar.generation.pipes;

import static org.assertj.core.api.Assertions.assertThat;

import de.claasklar.generation.ContextDocumentGeneratorBuilder;
import de.claasklar.primitives.CollectionName;
import de.claasklar.primitives.document.ArrayValue;
import de.claasklar.primitives.document.ByteValue;
import de.claasklar.primitives.document.DoubleValue;
import de.claasklar.primitives.document.Id;
import de.claasklar.primitives.document.OurDocument;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

public class ContextDocumentGeneratorTest {

  @Test
  public void testReferencesIdShouldBeIncluded() {
    // given
    var generator =
        ContextDocumentGeneratorBuilder.builder()
            .fieldFromPipe(
                "test_ids",
                fieldPipeBuilder ->
                    fieldPipeBuilder.selectCollection(
                        new CollectionName("test"),
                        pipeBuilder -> pipeBuilder.selectByPath("$.[*]._id").toArray()))
            .build();
    var documentId = randomId();
    var documents =
        new OurDocument[] {
          new OurDocument(randomId(), Collections.emptyMap()),
          new OurDocument(randomId(), Collections.emptyMap())
        };
    // when
    var document =
        generator.generateDocument(documentId, Map.of(new CollectionName("test"), documents));
    // then
    assertThat(document)
        .isEqualTo(
            new OurDocument(
                documentId,
                Map.of(
                    "test_ids",
                    new ArrayValue(
                        Arrays.asList(
                            new ByteValue(documents[0].getId().id()),
                            new ByteValue(documents[1].getId().id()))))));
  }

  @Test
  public void testRandomNumberShouldBeIncluded() {
    // given
    var generator =
        ContextDocumentGeneratorBuilder.builder()
            .field("constant", () -> new DoubleValue(5))
            .build();
    // when
    var document = generator.generateDocument(randomId(), Collections.emptyMap());
    // then
    assertThat(document.getValues().get("constant")).isEqualTo(new DoubleValue(5));
  }

  private Id randomId() {
    var bytes = new byte[12];
    new Random().nextBytes(bytes);
    return new Id(bytes);
  }
}
