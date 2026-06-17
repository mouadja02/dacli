# Vulture whitelist (P10). CI runs `vulture src .vulture_whitelist.py
# --min-confidence 80`; at that bar the dynamic-dispatch noise below is already
# silent, so this file is the audited record of *why* those symbols look unused
# to a static scan — pydantic fields, registry-loaded connector classes, op- and
# command-dispatch methods, and enum members reached by value/serialization.
# Seeded with `vulture src --make-whitelist` then hand-pruned to genuine
# dynamic-dispatch entries (real dead code is left to surface, not masked).
#
# Vulture parses this file for *names used*; the leading `_.` is its convention
# for an attribute/method on an arbitrary object.

# --- pydantic Settings fields (validated/serialized by name, not referenced
#     literally) + validators + model_config ---
top_p
presence_penalty
frequency_penalty
auto_approve_safe_ops
confirm_data_loads
confirm_destructive_ops
step_by_step_mode
log_level
save_history
syntax_highlighting
show_spinners
show_timing
table_format
truncate_output
model_config
_.validate_log_level

# --- skill spec dataclass fields (read via the skill registry) ---
can_do
cannot_do
output_schema
escalation_target

# --- enum members reached by .value / serialization, not by name ---
CANCELLED  # ToolStatus
TIMEOUT  # ToolStatus
USER  # memory store scope
SESSION  # memory store scope

# --- connectors instantiated by the registry from a manifest `class:` path ---
AirflowConnector
DagsterConnector
DbtConnector
DynamoDBConnector
McpBridgeConnector
MongoDBConnector
MySQLConnector
PostgresConnector
DataDiffSkill
MyCliConnector  # connector scaffold template
MyToolConnector  # connector scaffold template

# --- github operations dispatched by name (op -> _op_<name>) ---
_._op_read_file
_._op_delete_file
_._op_list_directory
_._op_create_or_update_workflow
_._op_list_workflow_runs
_._op_get_workflow_run
_._op_trigger_workflow
_._op_get_workflow_run_jobs
_._fetch_job_raw_log

# --- Click command callbacks (invoked through @cli.command decorators) ---
init
doctor
plan_cmd
diff_cmd
audit
connector_install
export_run
eval_cmd
run_cmd
replay_cmd

# --- chat slash handlers + prompt_toolkit callbacks (registered, not called
#     by name) ---
_help
_keys
_init
_status
_doctor
_usage
_history
_sessions
_catalog
_schema
_export
_theme
_prompt
_clear
_cls
_reset
_setup
_connect
_new_connector
_testmode
_import_connector
_push_connector
_debug_connector
_.get_completions

# --- lazy module attribute + theme key table ---
__getattr__
STYLE_KEYS
