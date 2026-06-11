"""Tests for `/init` DACLI.md draft generation and chat command autocompletion."""

import unittest
from pathlib import Path
from types import SimpleNamespace as NS

from click.testing import CliRunner
from prompt_toolkit.document import Document

from dacli.memory.priors import generate_dacli_md
from dacli.scripts.cli import SlashCommandCompleter, cli
from dacli.config import CLI_COMMANDS


def _settings(**overrides):
    base = NS(
        snowflake=NS(database="BITCOIN_DATA", db_schema="PUBLIC", warehouse="WH",
                     role="SYSADMIN", account="acct123", user="bob", password="SECRET_PW"),
        github=NS(owner="mouadja02", repo="dacli", branch="main", token="ghp_SECRET"),
        pinecone=NS(index_name="docs", environment="us-east-1", api_key="pc_SECRET"),
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


class InitDraftTest(unittest.TestCase):
    def test_profiles_drawn_from_config(self):
        md = generate_dacli_md(_settings())
        self.assertIn("`BITCOIN_DATA`", md)
        self.assertIn("`BITCOIN_DATA.PUBLIC`", md)
        self.assertIn("`mouadja02/dacli`", md)
        self.assertIn("`docs`", md)

    def test_no_secrets_or_identifiers_leak(self):
        md = generate_dacli_md(_settings())
        for secret in ("SECRET_PW", "ghp_SECRET", "pc_SECRET", "acct123", "bob"):
            self.assertNotIn(secret, md, f"draft leaked {secret!r}")

    def test_empty_config_degrades_gracefully(self):
        empty = NS(
            snowflake=NS(database="", db_schema="", warehouse="", role=""),
            github=NS(owner="", repo="", branch=""),
            pinecone=NS(index_name="", environment=""),
        )
        md = generate_dacli_md(empty)
        self.assertIn("No connectors configured yet", md)

    def test_draft_loads_as_priors_top_layer(self):
        md = generate_dacli_md(_settings())
        self.assertTrue(md.startswith("# DACLI Priors"))


class CompleterTest(unittest.TestCase):
    def _complete(self, text):
        completer = SlashCommandCompleter(CLI_COMMANDS)
        doc = Document(text, cursor_position=len(text))
        return [c.text for c in completer.get_completions(doc, None)]

    def test_prefix_completes_commands(self):
        self.assertIn("/init", self._complete("/in"))
        self.assertIn("/status", self._complete("/st"))

    def test_slash_lists_all_commands(self):
        completions = self._complete("/")
        self.assertIn("/init", completions)
        self.assertIn("/help", completions)
        self.assertIn("/exit", completions)

    def test_non_command_text_yields_nothing(self):
        self.assertEqual(self._complete("create a bronze table"), [])

    def test_no_completion_past_command_word(self):
        # Once the command word is typed and a space follows, stop completing.
        self.assertEqual(self._complete("/load "), [])

    def test_args_stripped_from_completion(self):
        # "/load <id>" in CLI_COMMANDS completes to just "/load".
        self.assertIn("/load", self._complete("/lo"))


class InitCommandTest(unittest.TestCase):
    """`dacli init` must write a config.yaml in a clean directory, never raise."""

    def test_init_writes_config_without_raising(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["init"], obj={})
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertTrue(Path("config.yaml").exists())

    def test_init_honours_custom_target_path(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["init", "--config", "custom.yaml"], obj={})
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertTrue(Path("custom.yaml").exists())


if __name__ == "__main__":
    unittest.main()
