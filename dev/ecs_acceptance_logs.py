"""Read bounded CloudWatch test streams, including empty intermediate pages."""


def stream_events(aws, group, stream):
    data = {"logGroupName": group, "logStreamName": stream, "startFromHead": True}
    events, seen = [], set()
    for _ in range(100):
        result = aws("logs", "get-log-events", data)
        events.extend(result["events"])
        token = result.get("nextForwardToken")
        if not token:
            raise RuntimeError("test log pagination token missing")
        if token == data.get("nextToken"):
            return events
        if token in seen:
            raise RuntimeError("test log pagination cycle")
        seen.add(token)
        data["nextToken"] = token
    raise RuntimeError("test log pagination budget exceeded; do not publish partial oracle")
