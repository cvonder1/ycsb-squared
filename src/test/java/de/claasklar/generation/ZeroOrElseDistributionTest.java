package de.claasklar.generation;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import de.claasklar.random.distribution.*;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class ZeroOrElseDistributionTest {

  private final StdRandomNumberGenerator stdRandomNumberGenerator = new StdRandomNumberGenerator();

  @Test
  @DisplayName("given pZero of 0.3 and 1000000 trials sample should return about 300000 zeros")
  public void sampleShouldReturnAround30000ZerosForP30() {
    // given
    var testSubject =
        new ZeroOrElseDistribution(0.3, new ConstantDistribution(1), stdRandomNumberGenerator);
    // when
    var zeroCount = 0;
    for (int i = 0; i < 1_000_000; i++) {
      if (testSubject.sample() == 0) {
        zeroCount++;
      }
    }
    // then
    assertThat(zeroCount).isCloseTo(300000, Percentage.withPercentage(1));
  }
}
