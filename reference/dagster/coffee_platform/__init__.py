"""The coffee chain's data platform, used as faultline's system under test.

This package must never import faultline: it only sees what a real platform
would see (its source database, its landing bucket, its warehouse).
"""
