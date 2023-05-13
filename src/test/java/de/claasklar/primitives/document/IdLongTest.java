package de.claasklar.primitives.document;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class IdLongTest {

  @Test
  public void testToIdShouldReturnSameValueFor0() {
    // given
    // when
    var first = new IdLong(0).toId();
    var second = new IdLong(0).toId();
    // then
    assertThat(first).isEqualTo(second);
  }

  @Test
  public void testToIdShouldReturnSameValueFor1() {
    // given
    // when
    var first = new IdLong(1).toId();
    var second = new IdLong(1).toId();
    // then
    assertThat(first).isEqualTo(second);
  }

  @Test
  public void testToIdShouldReturnDifferentValueFor100And201() {
    // given
    // when
    var first = new IdLong(100).toId();
    var second = new IdLong(201).toId();
    // then
    assertThat(first).isNotEqualTo(second);
  }
}
