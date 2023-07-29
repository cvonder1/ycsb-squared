package de.claasklar.generation;

import static org.assertj.core.api.Assertions.assertThat;

import de.claasklar.random.distribution.GeometricLongDistribution;
import de.claasklar.random.distribution.StdRandomNumberGenerator;
import java.util.stream.Stream;
import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class GeometricLongDistributionTest {

  @ParameterizedTest
  @ValueSource(doubles = {0.1, 0.5, 0.8, 1})
  public void testSampleShouldReturnValuesWithMeanOneOverP(double p) {
    // given
    var testSubject = new GeometricLongDistribution(p, new StdRandomNumberGenerator());
    // when
    var summary =
        Stream.generate(testSubject::sample)
            .limit(50_000)
            .mapToLong(Long::longValue)
            .summaryStatistics();
    // then
    assertThat(summary.getAverage()).isCloseTo((1 - p) / p, Offset.offset(0.1));
  }

  @ParameterizedTest
  @ValueSource(doubles = {0.1, 0.5, 0.8, 1})
  public void testSampleShouldReturnValuesWithVariance(double p) {
    // given
    var testSubject = new GeometricLongDistribution(p, new StdRandomNumberGenerator());
    // when
    var summary =
        new Object() {
          long n = 0;
          long sum = 0;
          long sumSq = 0;
        };
    var result =
        Stream.generate(testSubject::sample)
            .limit(1_000_000)
            .reduce(
                summary,
                (acc, it) -> {
                  summary.n++;
                  summary.sum += it;
                  summary.sumSq += it * it;
                  return summary;
                },
                (first, second) -> {
                  first.n = first.n + second.n;
                  first.sum = first.sum + second.sum;
                  first.sumSq = first.sumSq + second.sumSq;
                  return first;
                });
    double variance =
        (result.sumSq - (result.sum * result.sum) / (double) result.n) / (result.n - 1);
    // then
    assertThat(variance).isCloseTo((1 - p) / (p * p), Offset.offset(0.1 / p));
  }
}
