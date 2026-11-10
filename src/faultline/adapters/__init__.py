"""Adapters: how faultline drives one kind of data platform.

faultline itself never knows which tools a platform is built from. An adapter
translates a handful of generic operations (set up a sandbox, deliver a day of
input, run a partition, snapshot a table) into that platform's own calls.
"""
