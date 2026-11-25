"""Stop every owned proof process and refuse success when any cleanup fails."""
import sys


def cleanup_owned(devices, relays=()):
    errors = []
    for device in devices:
        try:
            device.node.close()
        except Exception as error:  # noqa: BLE001 finish the other owned processes before failing
            errors.append(f"{device.name}: {error}")
    for relay in relays:
        try:
            relay.stop()
        except Exception as error:  # noqa: BLE001 retain every cleanup failure
            errors.append(f"relay {relay.data.name}: {error}")
    if errors:
        for error in errors:
            print(f"cleanup failed: {error}", file=sys.stderr)
        raise RuntimeError("Owned proof cleanup failed: " + "; ".join(errors))
    print(f"Owned proof cleanup passed: {len(devices)} devices, {len(relays)} relays", flush=True)
