package de.claasklar.util;

import static io.opentelemetry.sdk.trace.samplers.Sampler.parentBased;
import static io.opentelemetry.sdk.trace.samplers.Sampler.traceIdRatioBased;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.List;
import java.util.Random;

public class TelemetryConfig {

  public static final String METRIC_SCOPE_NAME = "de.claasklar.ycsb-squared";
  public static final String INSTRUMENTATION_SCOPE_NAME = "de.claasklar.ycsb-squared";
  public static final String APPLICATION_SPAN_NAME = "complete_benchmark";
  private static String version = null;

  public static String version() {
    if (version == null) {
      version = Integer.toString(new Random().nextInt(0, Integer.MAX_VALUE));
    }
    return version;
  }

  public static OpenTelemetry buildOpenTelemetry() {
    Resource resource =
        Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "ycsb-squared")));

    SdkTracerProvider sdkTracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(
                BatchSpanProcessor.builder(
                        OtlpGrpcSpanExporter.builder().setEndpoint("http://jaeger:4317").build())
                    .build())
            .setResource(resource)
            .setSampler(new CustomAlwaysSampler(parentBased(traceIdRatioBased(0.02))))
            .build();

    SdkMeterProvider sdkMeterProvider =
        SdkMeterProvider.builder()
            .registerMetricReader(PrometheusHttpServer.builder().build())
            .setResource(resource)
            .build();

    return OpenTelemetrySdk.builder()
        .setTracerProvider(sdkTracerProvider)
        .setMeterProvider(sdkMeterProvider)
        .buildAndRegisterGlobal();
  }

  private record CustomAlwaysSampler(Sampler delegate) implements Sampler {

    @Override
    public SamplingResult shouldSample(
        Context parentContext,
        String traceId,
        String name,
        SpanKind spanKind,
        Attributes attributes,
        List<LinkData> parentLinks) {
      if (name == TelemetryConfig.APPLICATION_SPAN_NAME) {
        return SamplingResult.recordAndSample();
      }
      return delegate.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
    }

    @Override
    public String getDescription() {
      return "Decorator around an other sampler, that always selects program over-arching spans";
    }
  }
}
