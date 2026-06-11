"""ToolResult / ToolStatus serialization contract (roadmap 03).

`ToolStatus.SUCCESS` must carry the correctly spelled value `"success"`
(it historically dropped a "c"), and `ToolResult.to_dict` must serialize the enum
*value* — a plain string — not the enum object (which would render as
`"ToolStatus.SUCCESS"` under `json.dumps(..., default=str)`).
"""

import json
import unittest

from dacli.connectors.base import ToolResult, ToolStatus


class ToolStatusValueTest(unittest.TestCase):
    def test_success_value_is_spelled_correctly(self):
        self.assertEqual(ToolStatus.SUCCESS.value, "success")

    def test_all_values_are_lowercase_strings(self):
        for member in ToolStatus:
            self.assertIsInstance(member.value, str)
            self.assertEqual(member.value, member.value.lower())


class ToolResultToDictTest(unittest.TestCase):
    def test_status_serializes_as_value_string(self):
        result = ToolResult(tool_name="t", status=ToolStatus.SUCCESS)
        status = result.to_dict()["status"]
        self.assertIsInstance(status, str)
        self.assertEqual(status, "success")

    def test_to_dict_is_json_serializable_without_default_str(self):
        result = ToolResult(tool_name="t", status=ToolStatus.ERROR, error="boom")
        payload = json.loads(json.dumps(result.to_dict()))
        self.assertEqual(payload["status"], "error")
        self.assertNotIn("ToolStatus", json.dumps(payload))


if __name__ == "__main__":
    unittest.main()
