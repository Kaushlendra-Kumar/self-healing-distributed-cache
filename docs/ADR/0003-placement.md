# ADR 0003: Consistent hashing with virtual nodes

## Context
Keys must map deterministically to N owners. Clients and every node must agree given
the same membership. Membership changes should move as little data as possible.

## Options
- **Modulo hashing:** simplest, but remaps ~(n−1)/n of keys on every change. Measured:
  80% for 4→5 nodes.
- **Consistent hashing, one point per node:** moves ~1/n, but arcs are very uneven
  (64% relative std-dev measured).
- **Consistent hashing + virtual nodes:** moves ~1/n with even arcs; costs O(n·v) memory
  and a binary search per lookup.
- **Rendezvous (HRW) hashing:** perfectly even in expectation, with no ring state, but
  O(n) work per lookup to score every node. A natural alternative for small n.

## Decision
A ring with 128 virtual nodes per member (configurable). Hash: FNV-1a followed by the
splitmix64 finalizer, because plain FNV clusters similar inputs like `node-1#17`. Points
are sorted by (hash, node ID), so collisions resolve identically everywhere. Rings are
immutable and published through an atomic pointer.

## Consequences
- Measured: 19.9% remapped for 4→5 nodes (ideal 20%); 9.7% std-dev with 256 vnodes;
  `Owners` takes 119 ns.
- Every node and client must use the same vnode count; it is not negotiated.
- The preference list (owners in clockwise order) also defines replica placement and the
  coordinator order.
