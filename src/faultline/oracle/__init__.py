"""The oracle: what the right answer is, and how far an output is from it.

faultline's oracle is hybrid. A clean baseline run of the same platform on the
same input is the answer key (differential), and the generator's own key sets
check that the baseline itself is sane (invariants). All comparisons run in an
embedded DuckDB over Parquet snapshots, whatever warehouse produced them.
"""
