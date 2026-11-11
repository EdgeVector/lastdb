"""Appends and acks pay for #769's three server attempts. Head and list do not."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cost import (
    SERVER_ATTEMPTS, CapExceeded, Ledger, MUTATING_WRITE_UNITS, load_reads,
)
from drive import Session
from guard import database_id_for_run
from test_cost import MemoryLog


def _round(ledger, noted):
    for index in range(100):
        if not noted[index]:
            ledger.note_device("w{0}".format(index))
            noted[index] = True
        ledger.charge("write", 80, grow=True, attempts=SERVER_ATTEMPTS)
        ledger.charge("read")
        ledger.charge("read")
        ledger.charge("write", 80, grow=False, attempts=SERVER_ATTEMPTS)


class ServerAttempts(unittest.TestCase):
    def test_appends_and_acks_pay_for_three_server_attempts(self):
        session = Session(
            database_id_for_run("17"), "ab" * 16, "cd" * 32, MemoryLog(), Ledger(), lambda: 0)
        session.call("append", index=0, seq=1)
        self.assertGreater(session.ledger.partition_bytes, 800)
        self.assertEqual(session.ledger.reads, load_reads(800, 0) * SERVER_ATTEMPTS)
        self.assertEqual(session.ledger.write_units, MUTATING_WRITE_UNITS * SERVER_ATTEMPTS)
        before = session.ledger.reads
        session.call("head", index=0)
        self.assertEqual(
            session.ledger.reads - before, load_reads(session.ledger.partition_bytes, 0))
        held = session.ledger.partition_bytes
        writes = session.ledger.write_units
        before = session.ledger.reads
        session.call("ack", index=0, seq=1)
        self.assertEqual(session.ledger.partition_bytes, held)
        self.assertEqual(session.ledger.reads - before, load_reads(held, 0) * SERVER_ATTEMPTS)
        self.assertEqual(
            session.ledger.write_units - writes, MUTATING_WRITE_UNITS * SERVER_ATTEMPTS)
        fresh = Ledger()
        noted = [False] * 100
        fresh.charge("write", 40, grow=False)
        for _ in range(38):
            _round(fresh, noted)
        self.assertLess(fresh.estimate()["total_usd"], 6)
        completed = 38
        tripped = None
        for _ in range(80):
            try:
                _round(fresh, noted)
            except CapExceeded:
                tripped = completed + 1
                break
            completed += 1
        self.assertEqual(tripped, 66)


if __name__ == "__main__":
    unittest.main()
