-- Glug (src/gulp/glug/formatter.py, GlugJsonFormatter) writes "timestamp_msec",
-- an integer epoch in milliseconds. Fluent Bit's own PARSER Time_Format only
-- understands seconds-since-epoch (%s) — parsing a millisecond value with it
-- directly would misinterpret it and produce a date decades in the future.
-- This filter overrides the record's actual Fluent Bit event time from that
-- field instead, so @timestamp in the shipped OpenSearch document matches
-- when the log line was actually generated, not when Fluent Bit read it.
function set_event_time(tag, timestamp, record)
    local ts_msec = record["timestamp_msec"]
    if ts_msec == nil then
        -- no usable field: keep Fluent Bit's own ingestion-time unchanged
        return 0, timestamp, record
    end
    -- code 1 = record unchanged, but replace the event timestamp (seconds,
    -- fractional) with this one
    return 1, ts_msec / 1000, record
end
