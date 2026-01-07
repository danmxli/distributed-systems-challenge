# Efficient Fault-Tolerant Broadcast System

#### Specifications

**RPC: `broadcast`**

Requests that a value be broadcast out to all nodes in the cluster. The value is always an integer and is unique for each message:
```
{
  "type": "broadcast",
  "message": 1000
}
```
Responds with `broadcast_ok` message to acknowledge:
```
{
  "type": "broadcast_ok"
}
```

**RPC: `read`**

Requests that a node return all values that it has seen:
```
{
  "type": "read"
}
```
Responds with `read_ok` message with list of seen values:
```
{
  "type": "read_ok",
  "messages": [1, 8, 72, 25]
}
```

**RPC: `topology`**

Informs the node of who its neighbouring nodes are:
```
{
  "type": "topology",
  "topology": {
    "n1": ["n2", "n3"],
    "n2": ["n1"],
    "n3": ["n1"]
  }
}
```
Responds with `topology_ok` message to acknowledge:
```
{
  "type": "topology_ok"
}
```