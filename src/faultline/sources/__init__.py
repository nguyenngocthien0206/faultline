"""Fake sources faultline controls (injection point 1).

faultline replays generated business changes into a source system day by day,
so the platform's ingestion sees time pass the way it would in production. The
same control later lets faultline corrupt the source on purpose.
"""
