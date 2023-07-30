package de.claasklar.phase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import de.claasklar.random.distribution.RandomNumberGenerator;
import de.claasklar.specification.TopSpecification;
import de.claasklar.util.Pair;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.data.Percentage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class WeightedRandomTransactionPhaseTest {

  private final TopSpecification firstSpecification = specificationMock();
  private final TopSpecification secondSpecification = specificationMock();
  private final TopSpecification thirdSpecification = specificationMock();
  private final TopSpecification fourthSpecification = specificationMock();

  private final List<Pair<Double, TopSpecification>> specifications =
      List.of(
          new Pair<>(0.25, firstSpecification),
          new Pair<>(0.25, secondSpecification),
          new Pair<>(0.25, thirdSpecification),
          new Pair<>(0.25, fourthSpecification));

  private final Tracer tracer = OpenTelemetry.noop().getTracer("test");
  private final RandomNumberGenerator random = mock(RandomNumberGenerator.class);

  @BeforeEach
  public void setup() {
    when(random.nextDouble(0, 1))
        .then(
            new Answer<Double>() {
              final AtomicReference<Double> state = new AtomicReference<>(0.0);

              @Override
              public Double answer(InvocationOnMock invocation) throws Throwable {
                return state.getAndUpdate(old -> (old + 0.25) % 1);
              }
            });
  }

  @Test
  public void testRunShouldExecuteSpec1000Times() {
    // given
    var testSubject =
        new WeightedRandomTransactionPhase(
            1000,
            2,
            100,
            specifications,
            random,
            tracer.spanBuilder("test span").startSpan(),
            tracer);
    // when
    testSubject.run();
    // then
    verify(firstSpecification, times(250)).runnable();
    verify(secondSpecification, times(250)).runnable();
    verify(thirdSpecification, times(250)).runnable();
    verify(fourthSpecification, times(250)).runnable();
  }

  @Test
  public void testRunShouldTake2Seconds() {
    // given
    var testSubject =
        new WeightedRandomTransactionPhase(
            2000,
            2,
            1,
            specifications,
            random,
            tracer.spanBuilder("test span").startSpan(),
            tracer);
    // when
    var start = Clock.systemUTC().instant();
    testSubject.run();
    var duration = start.until(Clock.systemUTC().instant(), ChronoUnit.MILLIS);
    // then
    assertThat(duration).isCloseTo(2000, Percentage.withPercentage(20));
  }

  private TopSpecification specificationMock() {
    var specification = mock(TopSpecification.class);
    when(specification.runnable()).thenReturn(() -> {});
    when(specification.getName()).thenReturn("test_spec");
    return specification;
  }
}
