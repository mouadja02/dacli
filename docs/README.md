# dacli documentation

Reference documentation for **dacli**, the reliability-first data-engineering agent.
Start with the [project README](../README.md) for an overview and quick start.

## Contents

| Doc | What's inside |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | The six-component harness (ℛℳ𝒞𝒮𝒪𝒢), the microkernel, and the tool vs. sandbox execution tiers. |
| [CONNECTORS.md](CONNECTORS.md) | The connector catalog, the Definition of Done, and a step-by-step guide to adding a platform. |
| [GOVERNANCE.md](GOVERNANCE.md) | Blast-radius classification, the policy engine, rollback, the audit ledger, permissions, and the sandbox. |
| [EVALUATION.md](EVALUATION.md) | pass^k, golden suites, simulated platforms, regression detection, the dashboard, and self-improvement. |
| [CONFIGURATION.md](CONFIGURATION.md) | The full `config.yaml` reference, environment variables, and connector enablement. |

## Design background

dacli is built as a *harness-engineering* exercise, not a feature list. Its design is grounded in the
six-component harness framework from
*["From Model Scaling to System Scaling: Scaling the Harness in Agentic AI"](https://arxiv.org/abs/2605.26112)* —
see [ARCHITECTURE.md](ARCHITECTURE.md) for how each component (ℛℳ𝒞𝒮𝒪𝒢) is realized in the codebase.

## A note on the diagrams

Several docs use [Mermaid](https://mermaid.js.org/) diagrams, which GitHub renders natively. If you are
reading these outside GitHub, the fenced ```mermaid``` blocks are still readable as text.
