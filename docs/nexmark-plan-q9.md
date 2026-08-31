# Nexmark q9 — optimized plan tree

_winning bids — top bid per auction_

**Compile memo:** 0 shared-subplan hits, 6 misses (hits > 0 ⇒ CSE sharing reached the compiler).

## Operator counts (by plan-node kind)

| kind | count |
|:--|--:|
| ProjectPlan | 4 |
| ScanPlan | 2 |
| JoinPlan | 1 |
| PartitionedTopKPlan | 1 |

## Tree (`[shared #n]` = reference-identical to an earlier node)

```
ProjectPlan  (#0)
  JoinPlan (Inner, equiKeys=1)  (#1)
    ProjectPlan  (#2)
      ScanPlan (auction)  (#3)
    ProjectPlan  (#4)
      PartitionedTopKPlan  (#5)
        ProjectPlan  (#6)
          ScanPlan (bid)  (#7)
```
