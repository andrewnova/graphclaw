import os
import tempfile
import unittest
from pathlib import Path

from graphclaw.config import OrgConfig, validate_slug
from graphclaw.db import connect, stats


class GraphclawTests(unittest.TestCase):
    def test_validate_slug(self):
        self.assertEqual(validate_slug("OpenLaw"), "openlaw")
        with self.assertRaises(Exception):
            validate_slug("../bad")

    def test_db_init(self):
        with tempfile.TemporaryDirectory() as td:
            os.environ["GRAPHCLAW_HOME"] = td
            cfg = OrgConfig(org="testco", tenant="common", client_id="client", account="me", scopes=["User.Read"])
            conn = connect(cfg)
            data = stats(conn)
            self.assertEqual(data["raw_items"], 0)
            self.assertEqual(data["sync_cursors"], 0)


if __name__ == "__main__":
    unittest.main()

