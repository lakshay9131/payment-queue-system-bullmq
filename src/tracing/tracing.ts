import { trace, context, SpanStatusCode, Span } from '@opentelemetry/api';

const tracer = trace.getTracer('payment-processing-queue');

/**
 * Wraps a step in an OTel span, tagging it with the payment's correlation ID
 * so every span across enqueue -> process -> gateway call -> webhook can be
 * grepped/joined in the tracing backend by one field. correlationId is
 * generated once at payment creation and threaded through every queue job,
 * log line, and outbound gateway call header (X-Correlation-Id).
 */
export async function withSpan<T>(
  name: string,
  correlationId: string,
  attrs: Record<string, string | number | boolean>,
  fn: (span: Span) => Promise<T>,
): Promise<T> {
  return tracer.startActiveSpan(name, async (span) => {
    span.setAttribute('correlation_id', correlationId);
    for (const [k, v] of Object.entries(attrs)) span.setAttribute(k, v);
    try {
      const result = await fn(span);
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      throw err;
    } finally {
      span.end();
    }
  });
}

export function generateCorrelationId(): string {
  // In production, prefer a request-scoped ID passed in from the API
  // gateway (e.g. from an inbound X-Request-Id header) so the trace spans
  // the whole request lifecycle, not just the queue portion.
  return `corr_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}
