---
name: works-directly-on-main
description: "For dbsp-net, commit and push straight to main — do not create feature branches"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: c15359a4-9705-4a58-9fa3-24c406f8e5f9
---

For the dbsp-net repo, commit and push **directly to `main`**. Do not create
feature branches or PRs unless explicitly asked.

**Why:** It's a solo project (Curt + me, see [[dbspnet-overview]]); branches
and PRs add ceremony with no reviewer. The harness default is "branch first
when on the default branch," but Curt has durably authorized working on
`main` directly here.

**How to apply:** When asked to commit/push, stay on `main` — stage, commit
(with the `Co-Authored-By` trailer), and `git push origin main`. Still keep
each logical change in its own commit. Only branch if Curt asks for it.
