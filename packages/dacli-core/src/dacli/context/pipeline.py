"""Context Constructor wiring.

Builds the per-turn collaborators the kernel uses instead of a fixed window: a
token counter + budget, a selection-policy assembler (with progressive
disclosure + dynamic prompt), off-context result spill, and budget-pressure
compaction. Extracted from the host (:class:`core.host.DacliHost`) so it stays a
thin wiring object.
"""


from dacli.context.assembler import build_context
from dacli.context.budget import Budget
from dacli.context.sources.dbt_manifest import DbtManifestSource
from dacli.context.compaction import compact, needs_compaction
from dacli.context.disclosure import disclose
from dacli.context.spill import ResultStore, summarize_or_inline
from dacli.context.tokenizer import make_counter
from dacli.prompts.system_prompt import compose_system_prompt


def build_context_pipeline(settings, memory, registry, llm, system_connector) -> dict:
    """Construct the context collaborators and return them as hooks.

    Returns ``{"build", "spill", "maybe_compact", "counter", "budget"}``.
    ``build`` is also used by ``dacli context --explain`` to inspect an
    assembled context.
    """
    counter = make_counter(settings)
    budget = Budget.from_settings(settings)
    store = ResultStore(session_id=memory.session_id)

    # Late-bind collaborators the system connector needs (3.3 / 3.4).
    system_connector.bind_registry(registry)
    system_connector.bind_result_store(store)

    def _remember_compaction(note: str) -> None:
        # Persist a compaction summary to durable memory with provenance so a
        # folded fact is never lost (raw history also stays on disk).
        remember = getattr(memory, "remember_fact", None)
        if remember is not None:
            remember(note, source="compaction", tags=["compaction"])

    # The most recently assembled Context, cached once per turn so the bottom
    # toolbar's ctx % can read the real budget snapshot without re-assembling.
    last: dict = {"ctx": None}

    # Live-env layer (F-5): catalog cache entries plus, when a dbt project is
    # configured, models parsed from its target/manifest.json (mtime-cached).
    # With no dbt project this provider is exactly the assembler's default
    # catalog path, so existing behaviour is unchanged.
    from dacli.config.settings import ConnectorConfig

    dbt_project_dir = ConnectorConfig(settings, "dbt").get("project_dir", "") or ""
    dbt_source = DbtManifestSource(dbt_project_dir) if dbt_project_dir else None

    def _live_entries(_task):
        entries = []
        catalog = getattr(memory, "catalog", None)
        if catalog is not None:
            entries.extend(catalog.list_objects())
        if dbt_source is not None:
            entries.extend(dbt_source.entries())
        return entries

    def _build(task, working, disclosed):
        effective = disclose(task, registry, already_disclosed=disclosed)
        base = compose_system_prompt(task, effective)
        ctx = build_context(
            task,
            memory=memory,
            registry=registry,
            recent_messages=working,
            counter=counter,
            budget=budget,
            disclosed=effective,
            base_system_prompt=base,
            live_provider=_live_entries,
        )
        last["ctx"] = ctx
        return ctx

    def _spill(result) -> str:
        return summarize_or_inline(
            result, counter, settings.context.spill_threshold_tokens, store
        )

    async def _maybe_compact(working):
        if needs_compaction(
            working, counter, budget.total,
            pressure=settings.context.compaction_pressure,
        ):
            result = await compact(working, llm, store_fn=_remember_compaction)
            return result.messages
        return working

    return {
        "build": _build,
        "spill": _spill,
        "maybe_compact": _maybe_compact,
        "counter": counter,
        "budget": budget,
        "store": store,
        "last_context": lambda: last["ctx"],
    }
