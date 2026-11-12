"""Note markers shared by the updater and the daily digest. STDLIB ONLY - import nothing here.

send_digest runs as an `if: always()` step, also on a morning when the dependency install failed. It must be
able to read these markers without importing updater.orchestrate, which pulls pyarrow (review AR-477 r6).
One constant per marker, imported by both sides (R142's marker rule)."""

# A unit-error segment that starts with this is a real failure: a changed id or key of the pass got no
# health-read home (its booking or its unmapped-keys record failed). Written by orchestrate.run_once (review
# AR-477 r5 F1/F3); send_digest.kind_of files it as UNHOMED, before SUBSET.
UNHOMED_NOTE = "csv debt has NO HOME"
