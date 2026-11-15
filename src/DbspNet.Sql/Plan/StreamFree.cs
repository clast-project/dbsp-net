// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

// Logical plans are immutable records the resolver builds before PlanToCircuit creates any stream,
// so none can reach one. Compiled closures capture them by the hundred thousand (a plan's schema
// columns, projections, aggregate calls), and the stream-release scan walked every one on each new
// program's first tick — ~170 ms per worker after an ivm-bench restore (issue #8).
[assembly: DbspNet.Core.Circuit.StreamFreeNamespace("DbspNet.Sql.Plan")]
