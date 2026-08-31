---
name: docker-runs-in-wsl
description: "Docker isn't available to the agent's shells; Curt runs Docker commands himself in a separate WSL session from instructions I provide"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 25fe3083-66ab-4999-996e-242a97a89703
---

Docker is NOT on PATH in the agent's Bash (Git Bash) or PowerShell, and Curt does
NOT want the agent invoking `docker` (a PowerShell attempt was rejected). Curt runs
Docker himself in a **separate WSL session**, based on step-by-step commands I give him.

**Why:** the ivm-bench comparison harness (docker-compose, TPC-DI datagen, engine
containers, Feldera) runs under Docker in WSL on Curt's machine, out-of-band from this
session.

**How to apply:** for anything Docker-related (build engine images, run the ivm-bench
benchmark/comparison), DON'T call `docker`/`docker compose` via Bash or PowerShell.
Instead, hand Curt the exact commands to paste into his WSL session, and have him report
results back. Give copy-pasteable, ordered commands with the right working dir. Related:
[[ivm-bench-arc]], [[ivm-bench-validation-findings]].
