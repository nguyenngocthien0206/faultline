"""Thin clients for the systems a platform under test is built from.

They speak only public protocols (Dagster GraphQL, ClickHouse HTTP, Postgres)
and know nothing about faultline's faults or oracle, so adapters for any
platform built on these systems can reuse them.
"""
