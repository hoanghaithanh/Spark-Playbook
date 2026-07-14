"""Spark Playbook — annotation engine package (PLAN.md §3, §4).

Self-check-only per G3: static plan analysis (`plan_parser.py`) + manifest-
driven concept mapping (`engine.py`) + manifest loading/validation
(`manifest.py`). Nothing in this package is invoked automatically -- it is
only exercised on an explicit learner "Reveal" action (see
`app/web/routes/annotation.py`).
"""
