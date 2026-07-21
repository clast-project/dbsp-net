# Nexmark q5 — optimized plan tree

_hot items — sliding-window auction popularity_

**Compile memo:** 1 shared-subplan hits, 14 misses (hits > 0 ⇒ CSE sharing reached the compiler).

## Operator counts (by plan-node kind)

| kind | count |
|:--|--:|
| ProjectPlan | 10 |
| AggregatePlan | 2 |
| JoinPlan | 1 |
| ScanPlan | 1 |
| UnionAllPlan | 1 |

## Tree (`[shared #n]` = reference-identical to an earlier node)

```
ProjectPlan  (#0)
  JoinPlan (Inner, equiKeys=2, +residual)  (#1)
    ProjectPlan  (#2)
      AggregatePlan (groupKeys=3, aggs=1)  (#3)
        ProjectPlan  (#4)
          UnionAllPlan (branches=5)  (#5)
            ProjectPlan  (#6)
              ScanPlan (bid)  (#7)
            ProjectPlan  (#8)
              ScanPlan [shared #7]
            ProjectPlan  (#9)
              ScanPlan [shared #7]
            ProjectPlan  (#10)
              ScanPlan [shared #7]
            ProjectPlan  (#11)
              ScanPlan [shared #7]
    ProjectPlan  (#12)
      AggregatePlan (groupKeys=2, aggs=1)  (#13)
        ProjectPlan  (#14)
          AggregatePlan [shared #3]
```
