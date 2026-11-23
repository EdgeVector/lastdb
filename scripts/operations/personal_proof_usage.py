"""A proof device that keeps the account of what it put on the relay, across the homes the proof wipes.

`log-sync/<db>/cloud-usage.json` lives in a home. Some steps stop a device, delete its home and restore it
from the phrase; the entries that home put stay on the relay. The last account of each home is kept here, so
`personal_proof_reconcile` can add up everything the relay holds.
"""
import json

from personal_proof_nodes import Device


class AccountedDevice(Device):
    def __init__(self, *arguments):
        super().__init__(*arguments)
        self.retired, self.last_usage = [], None

    def restore(self, phrase):
        if self.last_usage is not None:
            self.retired.append(self.last_usage)
            self.last_usage = None
        return super().restore(phrase)

    def stop(self):
        super().stop()
        self.last_usage = self.usage() or self.last_usage

    def usage(self):
        """The account of what this home put on the relay, or None."""
        try:
            return json.loads((self.log_dir() / "cloud-usage.json").read_text())
        except (OSError, ValueError):
            return None

    def all_usage(self):
        """The accounts of every home this device had, oldest first, the current one last."""
        current = self.usage() or self.last_usage
        return [*self.retired, *([current] if current else [])]
