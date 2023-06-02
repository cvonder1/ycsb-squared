package de.claasklar.database.mongodb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import de.claasklar.database.mongodb.codec.OurDocumentCodecRegistry;
import de.claasklar.primitives.document.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.bson.*;
import org.bson.codecs.DecoderContext;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

public class OurDocumentCodecRegistryTest {

  private final OurDocumentCodecRegistry testSubject = new OurDocumentCodecRegistry();

  @Test
  public void testToBsonDocumentShouldInsertId() {
    // given
    var ourDocument = new OurDocument(new IdLong(10).toId(), Collections.emptyMap());
    // when
    var result = BsonDocumentWrapper.asBsonDocument(ourDocument, testSubject);
    // then
    assertThat(result)
        .containsExactly(entry("_id", new BsonObjectId(new ObjectId(ourDocument.getId().id()))));
  }

  @Test
  public void testToBsonDocumentShouldInsertNumberValue() {
    // given
    var ourDocument = new OurDocument(randomId(), Map.of("num", new DoubleValue(40.5f)));
    // when
    var result = BsonDocumentWrapper.asBsonDocument(ourDocument, testSubject);
    // then
    assertThat(result).contains(entry("num", new BsonDouble(40.5)));
  }

  @Test
  public void testToBsonDocumentShouldInsertBoolValue() {
    // given
    var ourDocument = new OurDocument(randomId(), Map.of("bool", new BoolValue(true)));
    // when
    var result = BsonDocumentWrapper.asBsonDocument(ourDocument, testSubject);
    // then
    assertThat(result).contains(entry("bool", new BsonBoolean(true)));
  }

  @Test
  public void testToBsonDocumentShouldInsertArrayValue() {
    // given
    var ourDocument =
        new OurDocument(
            randomId(),
            Map.of(
                "arr",
                new ArrayValue(Arrays.asList(new DoubleValue(1000.25f), new BoolValue(true)))));
    // when
    var result = BsonDocumentWrapper.asBsonDocument(ourDocument, testSubject);
    // then
    assertThat(result)
        .contains(
            entry(
                "arr",
                new BsonArray(Arrays.asList(new BsonDouble(1000.25f), new BsonBoolean(true)))));
  }

  @Test
  public void testToBsonDocumentShouldInsertStringValue() {
    // given
    var ourDocument = new OurDocument(randomId(), Map.of("str", new StringValue("hello world")));
    // when
    var result = BsonDocumentWrapper.asBsonDocument(ourDocument, testSubject);
    // then
    assertThat(result).contains(entry("str", new BsonString("hello world")));
  }

  @Test
  public void testToBsonDocumentShouldInsertNestedObjectValue() {
    // given
    var ourDocument =
        new OurDocument(
            randomId(),
            Map.of("doc", new NestedObjectValue(Map.of("str", new StringValue("hello")))));
    // when
    var result = BsonDocumentWrapper.asBsonDocument(ourDocument, testSubject);
    // then
    assertThat(result).containsKey("doc");
    assertThat(result.getDocument("doc")).contains(entry("str", new BsonString("hello")));
  }

  @Test
  public void testToBsonDocumentShouldInsertByteValue() {
    // given
    var ourDocument =
        new OurDocument(randomId(), Map.of("byte", new ByteValue(new byte[] {1, 2, 3, 4})));
    // when
    var result = BsonDocumentWrapper.asBsonDocument(ourDocument, testSubject);
    // then
    assertThat(result).contains(entry("byte", new BsonBinary(new byte[] {1, 2, 3, 4})));
  }

  @Test
  public void testToBsonDocumentShouldInsertNullValue() {
    // given
    var ourDocument = new OurDocument(randomId(), Map.of("null", NullValue.VALUE));
    // when
    var result = BsonDocumentWrapper.asBsonDocument(ourDocument, testSubject);
    // then
    assertThat(result).contains(entry("null", BsonNull.VALUE));
  }

  @Test
  public void testToBsonDocumentShouldInsertInt32() {
    // given
    var ourDocument = new OurDocument(randomId(), Map.of("int", new IntValue(34)));
    // when
    var result = BsonDocumentWrapper.asBsonDocument(ourDocument, testSubject);
    // then
    assertThat(result).contains(entry("int", new BsonInt32(34)));
  }

  @Test
  public void testDecodeBsonDocumentShouldSetId() {
    // given
    var id = randomId();
    var bson = new BsonDocument("_id", new BsonObjectId(new ObjectId(id.id())));
    // when
    var result =
        testSubject
            .get(OurDocument.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result).isNotNull();
    assertThat(result).extracting(OurDocument::getId).isEqualTo(id);
  }

  @Test
  public void testDecodeBsonDocumentWithoutIdShouldThrowError() {
    // given
    var bson = new BsonDocument();
    // when
    assertThatThrownBy(
            () ->
                testSubject
                    .get(OurDocument.class)
                    .decode(bson.asBsonReader(), DecoderContext.builder().build()))
        // then
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testDecodeShouldInsertString() {
    // given
    var bson =
        new BsonDocument(
            Arrays.asList(
                new BsonElement("_id", randomBsonObjectId()),
                new BsonElement("str", new BsonString("hello world"))));
    // when
    var result =
        testSubject
            .get(OurDocument.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result).isNotNull();
    assertThat(result.getValues()).contains(entry("str", new StringValue("hello world")));
  }

  @Test
  public void testDecodeShouldInsertArray() {
    // given
    var bson =
        new BsonDocument(
            Arrays.asList(
                new BsonElement("_id", randomBsonObjectId()),
                new BsonElement(
                    "arr",
                    new BsonArray(Collections.singletonList(new BsonString("hello world"))))));
    // when
    var result =
        testSubject
            .get(OurDocument.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result).isNotNull();
    assertThat(result.getValues()).containsKey("arr");
    assertThat((ArrayValue) result.get("arr")).contains(new StringValue("hello world"));
  }

  @Test
  public void testDecodeShouldDecodeNestedArray() {
    // given
    var bson =
        new BsonDocument(
            List.of(
                new BsonElement("_id", randomBsonObjectId()),
                new BsonElement(
                    "arr",
                    new BsonArray(
                        List.of(
                            new BsonArray(List.of(new BsonString("hello world"))),
                            new BsonString("outer hello"))))));
    // when
    var result =
        testSubject
            .get(OurDocument.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result.get("arr")).isNotNull();
    assertThat(result.get("arr"))
        .isEqualTo(
            new ArrayValue(
                List.of(
                    new ArrayValue(List.of(new StringValue("hello world"))),
                    new StringValue("outer hello"))));
  }

  @Test
  public void testDecodeShouldInsertBoolValue() {
    // given
    var bson =
        new BsonDocument(
            Arrays.asList(
                new BsonElement("_id", randomBsonObjectId()),
                new BsonElement("bool", new BsonBoolean(true))));
    // when
    var result =
        testSubject
            .get(OurDocument.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result).isNotNull();
    assertThat(result.getValues()).contains(entry("bool", new BoolValue(true)));
  }

  @Test
  public void testDecodeShouldInsertByteValue() {
    // given
    var bson =
        new BsonDocument(
            Arrays.asList(
                new BsonElement("_id", randomBsonObjectId()),
                new BsonElement("byte", new BsonBinary(new byte[] {1, 2, 3, 4}))));
    // when
    var result =
        testSubject
            .get(OurDocument.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result).isNotNull();
    assertThat(result.getValues()).contains(entry("byte", new ByteValue(new byte[] {1, 2, 3, 4})));
  }

  @Test
  public void testDecodeShouldInsertFloatValue() {
    // given
    var bson =
        new BsonDocument(
            Arrays.asList(
                new BsonElement("_id", randomBsonObjectId()),
                new BsonElement("float", new BsonDouble(2d))));
    // when
    var result =
        testSubject
            .get(OurDocument.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result).isNotNull();
    assertThat(result.getValues()).contains(entry("float", new DoubleValue(2f)));
  }

  @Test
  public void testDecodeShouldInsertNestObjectValue() {
    // given
    var bson =
        new BsonDocument(
            Arrays.asList(
                new BsonElement("_id", randomBsonObjectId()),
                new BsonElement("doc", new BsonDocument("double", new BsonDouble(2.5d)))));
    // when
    var result =
        testSubject
            .get(OurDocument.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result).isNotNull();
    assertThat(result.getValues())
        .contains(entry("doc", new NestedObjectValue(Map.of("double", new DoubleValue(2.5d)))));
  }

  @Test
  public void testDecodeShouldInsertNullValue() {
    // given
    var bson =
        new BsonDocument(
            (List.of(
                new BsonElement("_id", randomBsonObjectId()),
                new BsonElement("null", BsonNull.VALUE))));
    // when
    var result =
        testSubject
            .get(OurDocument.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result).isNotNull();
    assertThat(result.getValues()).contains(entry("null", NullValue.VALUE));
  }

  @Test
  public void testDecodeShouldInsertNullValueIntoNestedObjectValue() {
    // given
    var bson =
        new BsonDocument(
            Arrays.asList(
                new BsonElement("_id", randomBsonObjectId()),
                new BsonElement("null", BsonNull.VALUE),
                new BsonElement("doc", new BsonDocument("double", new BsonDouble(2.5d)))));
    // when
    var result =
        testSubject
            .get(NestedObjectValue.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result).isNotNull();
    assertThat(result.entrySet())
        .contains(entry("doc", new NestedObjectValue(Map.of("double", new DoubleValue(2.5d)))));
    assertThat(result.entrySet()).contains(entry("null", NullValue.VALUE));
  }

  @Test
  public void testDecodeShouldInsertIntValue() {
    // given
    var bson =
        new BsonDocument(
            List.of(
                new BsonElement("_id", randomBsonObjectId()),
                new BsonElement("num", new BsonInt32(506))));
    // when
    var result =
        testSubject
            .get(OurDocument.class)
            .decode(bson.asBsonReader(), DecoderContext.builder().build());
    // then
    assertThat(result.entrySet()).contains(entry("num", new IntValue(506)));
  }

  private Id randomId() {
    return new IdLong(new Random().nextLong()).toId();
  }

  private BsonObjectId randomBsonObjectId() {
    return new BsonObjectId(new ObjectId(randomId().id()));
  }
}
