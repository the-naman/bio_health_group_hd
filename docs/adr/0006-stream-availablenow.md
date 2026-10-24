# 0006 Real-time stream as scheduled availableNow runs

- Status: Accepted (23 Oct 2026)
- Context: payment events arrive all day on Kafka; Free Edition serverless cannot keep a stream running.
- Decision: Structured Streaming with `trigger(availableNow=True)` every 2 hours: Bronze raw, Silver with a 15-minute
  watermark and de-duplication, Gold 10-minute failure windows. Only the Gold windows table is masked, because Databricks
  cannot stream from a masked table; Bronze and Silver hold synthetic references only.
- Consequences: same code runs continuously on paid compute by changing the trigger. Windows close only when newer events
  arrive, so an alert can come one run later.
