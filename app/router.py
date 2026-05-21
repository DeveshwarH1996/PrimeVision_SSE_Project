import random

from app.models import PackageState

_ROUTES = ["BIN-A", "BIN-B", "BIN-C"]


def make_routing_decision(package: PackageState) -> tuple[str, float]:
    route = random.choice(_ROUTES)
    confidence = random.random()
    return route, confidence
