"""Context Constructor (𝒞).

Turns every turn's context into the output of a *selection policy* rather than a
fixed message window: persistent priors + just-in-time memory retrieval +
live-environment facts, packed under a token budget, provenance-tagged,
compacted for long horizons, with connectors progressively disclosed.

Public surface (built up across workstreams 3.1–3.6):

- :mod:`context.disclosure`  — progressive disclosure of connectors (3.3)
- :mod:`context.assembler`   — ``build_context`` selection-policy assembler (3.1)
- :mod:`context.tokenizer`   — token counting (3.2)
- :mod:`context.budget`      — per-source budget accounting (3.2)
- :mod:`context.spill`       — off-context spill of large results (3.4)
- :mod:`context.compaction`  — budget-pressure compaction (3.5)
"""
