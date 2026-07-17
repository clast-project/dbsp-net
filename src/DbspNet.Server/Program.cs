// Copyright (c) clast-project. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.
using DbspNet.Server;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<DbspNetEngine>();

var app = builder.Build();

// Health — the compose healthcheck / dbt-server depends_on polls this.
app.MapGet("/healthz", () => Results.Ok(new { status = "ok" }));

// Dry-run compile check (no connectors) — validate the model DAG compiles as one circuit.
app.MapPost("/compile", (CompileSpec spec) => Results.Ok(DbspNetEngine.Compile(spec)));

// Deploy a program (SQL DAG + Delta input/output bindings). Compiles + wires connectors.
app.MapPost("/deploy", async (ProgramSpec spec, DbspNetEngine engine, CancellationToken ct) =>
    Results.Ok(await engine.DeployAsync(spec, ct)));

// Start ingesting the current batch (timer start).
app.MapPost("/resume", (DbspNetEngine engine) => Results.Ok(engine.Resume()));

// Block until the batch has drained and its outputs are written (timer stop).
app.MapPost("/wait", async (DbspNetEngine engine, CancellationToken ct) =>
    Results.Ok(await engine.WaitAsync(ct)));

// Pause is a no-op: a DbspNet batch drains to completion, so there is nothing to stop.
app.MapPost("/pause", () => Results.Ok(new { status = "ok" }));

// Point-in-time stats (for polling / debugging).
app.MapGet("/stats", (DbspNetEngine engine) => Results.Ok(engine.GetStats()));

app.Run();
