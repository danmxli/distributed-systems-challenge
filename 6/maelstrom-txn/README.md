# Read Committed KV Store

### Specifications

**RPC: `txn`**

This message passes in an array of operations. An operation is represented by a 3-element array containing operation name, the integer key to operate on, and a possibly-null integer value.
```
{
  "type": "txn",
  "msg_id": 3,
  "txn": [
    ["r", 1, null],
    ["w", 1, 6],
    ["w", 2, 9]
  ]
}
```
The above example describes read from key 1, write value 6 to key 1, and write value 9 to key 2. Responds with `txn_ok` message with array of operations, where read operations have their value filled in:
```
{
  "type": "txn_ok",
  "msg_id": 1,
  "in_reply_to": 3,
  "txn": [
    ["r", 1, 3],
    ["w", 1, 6],
    ["w", 2, 9]
  ]
}
```

### Tests

Single-node system:
```bash
maelstrom test -w txn-rw-register --bin maelstrom-txn --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
```

Multi-node system:
```bash
maelstrom test -w txn-rw-register --bin maelstrom-txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted
```

Total availability under network partitions:
```bash
maelstrom test -w txn-rw-register --bin maelstrom-txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
```

Read committed consistency model:
```bash
maelstrom test -w txn-rw-register --bin maelstrom-txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition
```