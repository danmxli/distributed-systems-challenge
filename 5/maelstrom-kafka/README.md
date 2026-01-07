# Kafka-Style Log

### Specifications

**RPC: `send`**

Requests that a `"msg"` value be appended to a log identified by `"key"`:
```
{
  "type": "send",
  "key": "k1",
  "msg": 123
}
```
Responds with `send_ok` message to acknowledge:
```
{
  "type": "send_ok",
  "offset": 1000
}
```

**RPC: `poll`**

Requests that a node return messages from a set of logs starting from the given offset in each log:
```
{
  "type": "poll",
  "offsets": {
    "k1": 1000,
    "k2": 2000
  }
}
```
Responds with `poll_ok` message with `msgs` starting from the given offset for each log:
```
{
  "type": "poll_ok",
  "msgs": {
    "k1": [[1000, 9], [1001, 5], [1002, 15]],
    "k2": [[2000, 7], [2001, 2]]
  }
}
```

**RPC: `commit_offsets`**

Informs the node that messages have been successfully processed up to and including the given offset:
```
{
  "type": "commit_offsets",
  "offsets": {
    "k1": 1000,
    "k2": 2000
  }
}
```
Responds with `commit_offsets_ok` to acknowledge:
```
{
  "type": "commit_offsets_ok"
}
```

**RPC: `list_commited_offsets`**

Returns a map of committed offsets for a given set of logs. Clients use this to figure out where to start consuming from in a given log:
```
{
  "type": "list_committed_offsets",
  "keys": ["k1", "k2"]
}
```
Responds with `list_committed_offsets_ok` message containing a map of offsets for each requested key:
```
{
  "type": "list_committed_offsets_ok",
  "offsets": {
    "k1": 1000,
    "k2": 2000
  }
}
```