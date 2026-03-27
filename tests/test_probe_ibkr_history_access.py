from __future__ import annotations

import unittest

from src.tools.probe_ibkr_history_access import _classify_probe_result


class ProbeIbkrHistoryAccessTests(unittest.TestCase):
    def test_classify_probe_result_marks_permission_denied(self):
        status, diagnosis = _classify_probe_result(
            contract_details_count=1,
            history_bar_count=0,
            error_rows=[{"code": 162, "message": "No market data permissions for IBIS STK"}],
        )
        self.assertEqual(status, "NO_MARKET_DATA_PERMISSION")
        self.assertIn("历史权限不足", diagnosis)

    def test_classify_probe_result_marks_missing_security_definition(self):
        status, diagnosis = _classify_probe_result(
            contract_details_count=0,
            history_bar_count=0,
            error_rows=[{"code": 200, "message": "No security definition has been found"}],
        )
        self.assertEqual(status, "NO_SECURITY_DEF")
        self.assertIn("合约定义不存在", diagnosis)

    def test_classify_probe_result_marks_ok_when_bars_exist(self):
        status, diagnosis = _classify_probe_result(
            contract_details_count=1,
            history_bar_count=120,
            error_rows=[],
        )
        self.assertEqual(status, "OK")
        self.assertIn("历史日线都可正常获取", diagnosis)


if __name__ == "__main__":
    unittest.main()
