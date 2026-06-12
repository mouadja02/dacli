"""A-3 — one source of truth for the verb → tier vocabulary.

The action/SQL classifier and the shell command classifier consume different
inputs (an ``OperationSpec`` + SQL vs. a free-text shell line), but they must
agree on what a verb *means*. These tests pin that agreement **by
construction**: both classifiers import the same ``governance.vocab`` tables,
and a representative destructive verb (``drop``) classifies irreversible on
both paths.

    python -m unittest tests.test_governance_vocab
"""

import unittest

from dacli.governance import vocab
from dacli.governance.classifier import Tier, classify_sql
from dacli.governance.command_classifier import classify_command


class SharedVocabByConstructionTest(unittest.TestCase):
    """Both classifiers read the same module-level tables — drift is impossible."""

    def test_classifiers_share_the_vocab_objects(self):
        from dacli.governance import classifier, command_classifier

        self.assertIs(classifier._SQL_KEYWORD_TIERS, vocab.SQL_KEYWORD_TIERS)
        self.assertIs(command_classifier._SQL_IRREVERSIBLE, vocab.DESTRUCTIVE_SQL_RE)
        self.assertIs(command_classifier._IRREVERSIBLE_SUBVERBS, vocab.IRREVERSIBLE_SUBVERBS)
        self.assertIs(command_classifier._RISKY_SUBVERBS, vocab.RISKY_SUBVERBS)
        self.assertIs(command_classifier._WRITE_SUBVERBS, vocab.WRITE_SUBVERBS)
        self.assertIs(command_classifier._READ_SUBVERBS, vocab.READ_SUBVERBS)

    def test_tier_is_the_same_enum_everywhere(self):
        from dacli import governance

        self.assertIs(Tier, vocab.Tier)
        self.assertIs(governance.Tier, vocab.Tier)

    def test_destructive_verbs_declared_irreversible_in_both_tables(self):
        sql_irreversible = {
            kw for tier, kws in vocab.SQL_KEYWORD_TIERS
            if tier is Tier.IRREVERSIBLE for kw in kws
        }
        for verb in ("DROP", "TRUNCATE"):
            self.assertIn(verb, sql_irreversible)
            self.assertIn(verb.lower(), vocab.IRREVERSIBLE_SUBVERBS)


class ClassifierAgreementTest(unittest.TestCase):
    """A representative verb classifies identically through both inputs."""

    def test_drop_is_irreversible_as_sql(self):
        self.assertEqual(classify_sql("DROP TABLE customers").tier, Tier.IRREVERSIBLE)

    def test_drop_is_irreversible_as_cli_subverb(self):
        verdict = classify_command("bq rm -f dataset.table")
        self.assertEqual(verdict.tier, Tier.IRREVERSIBLE)

    def test_drop_is_irreversible_as_embedded_sql_in_shell(self):
        verdict = classify_command('snowsql -q "DROP TABLE customers"')
        self.assertEqual(verdict.tier, Tier.IRREVERSIBLE)
        self.assertTrue(verdict.irreversible)


if __name__ == "__main__":
    unittest.main()
