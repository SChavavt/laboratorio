"""Match legacy numeric log IDs without merging distinct order identifiers."""
import re
from collections import defaultdict


def identifier_resolver(identifiers):
    known = {str(value).strip() for value in identifiers if value is not None}
    numeric = defaultdict(set)
    for identifier in known:
        if re.fullmatch(r"[0-9]+", identifier):
            numeric[identifier.lstrip("0") or "0"].add(identifier)

    def resolve(value):
        identifier = str(value).strip() if value is not None else ""
        if identifier in known:
            return identifier
        # USER_ENTERED stripped leading zeros in historical log rows. Only
        # recover an unambiguous match in this sheet; never match by doctor.
        if re.fullmatch(r"[0-9]+", identifier):
            candidates = numeric.get(identifier.lstrip("0") or "0", set())
            if len(candidates) == 1:
                return next(iter(candidates))
        return identifier

    return resolve
