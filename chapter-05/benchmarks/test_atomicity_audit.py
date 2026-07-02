"""Unit tests for atomicity_audit.py logic.

Validates that the audit's filter and detection logic correctly distinguish
imperative-compliant outcomes (whole present, whole absent) from violations
(partial). These tests run without any external services.
"""

import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import atomicity_audit as aa


class AuditTests(unittest.TestCase):
    def setUp(self):
        random.seed(42)
        self.traces = aa._gen_traces(100, 8)

    def test_none_mode_all_whole(self):
        emitted = aa.FAILURE_FILTERS["none"](self.traces, 0.05)
        audit = aa._audit(emitted, 8)
        audit["absent"] += len(self.traces) - len(emitted)
        self.assertEqual(audit["partial"], 0)
        self.assertEqual(audit["whole"], 100)
        self.assertEqual(audit["absent"], 0)

    def test_producer_crash_keeps_imperative(self):
        emitted = aa.FAILURE_FILTERS["producer-crash"](self.traces, 0.20)
        audit = aa._audit(emitted, 8)
        audit["absent"] += len(self.traces) - len(emitted)
        self.assertEqual(audit["partial"], 0,
                         "producer-crash drops whole batches, partial=0 expected")
        self.assertGreater(audit["absent"], 0)
        self.assertGreater(audit["whole"], 0)

    def test_drop_whole_trace_keeps_imperative(self):
        emitted = aa.FAILURE_FILTERS["drop-whole-trace"](self.traces, 0.30)
        audit = aa._audit(emitted, 8)
        audit["absent"] += len(self.traces) - len(emitted)
        self.assertEqual(audit["partial"], 0,
                         "drop-whole-trace evicts entire trace state, partial=0 expected")
        self.assertEqual(audit["whole"] + audit["absent"], 100)

    def test_buffer_overflow_violates_imperative(self):
        emitted = aa.FAILURE_FILTERS["buffer-overflow"](self.traces, 0.30)
        audit = aa._audit(emitted, 8)
        audit["absent"] += len(self.traces) - len(emitted)
        self.assertGreater(audit["partial"], 0,
                           "buffer-overflow evicts random spans, partial>0 expected")


if __name__ == "__main__":
    unittest.main()
