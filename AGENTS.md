# kafka4php — Agent guide

kafka4php is a Rust + C Kafka consumer host that embeds PHP (ZTS) and runs each Kafka
message as one isolated PHP request, like PHP-FPM does for HTTP.

## Read before doing anything

1. **[docs/DESIGN.md](docs/DESIGN.md)** — how the system works. Authoritative.
2. **[docs/PLAN.md](docs/PLAN.md)** — the implementation steps, their dependencies,
   status, and the protocol for claiming and finishing a step.

## Rules

- Work on **one plan step** at a time, and only if all of its dependencies are `done`.
  Claim it in PLAN.md first, as PLAN.md describes.
- **DESIGN.md must stay true.** Any behaviour or interface change updates DESIGN.md
  in the same commit.
- `k4p-host` and `k4p-php` must never depend on each other; they meet only through
  `k4p-core` and the `kafka4php` binary.
- The dev machine is **Windows**; the product is **Linux-only**. Build and test
  everything inside the toolchain container (`scripts/dev.sh` / `scripts/dev.ps1`,
  created in step S1). Keep files LF.
- A step isn't done while any test is failing or skipped.
