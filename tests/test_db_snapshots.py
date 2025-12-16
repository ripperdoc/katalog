from __future__ import annotations

from katalog.db import Database

PROVIDER_ID = "snapshots-test"


def _make_database() -> Database:
    db = Database(":memory:")
    db.initialize_schema()
    db.ensure_source(
        PROVIDER_ID,
        title="Snapshots Test",
        plugin_id="tests.snapshots",
        config={},
        provider_type="source",
    )
    return db


def _complete_snapshot(db: Database, status: str) -> int:
    snapshot = db.begin_snapshot(PROVIDER_ID)
    db.finalize_snapshot(snapshot, status=status)
    return snapshot.id


def test_cutoff_requires_full_snapshot():
    db = _make_database()
    _complete_snapshot(db, "partial")
    assert db.get_cutoff_snapshot(PROVIDER_ID) is None
    db.close()


def test_cutoff_prefers_newest_partial_or_full_after_full():
    db = _make_database()
    full_id = _complete_snapshot(db, "full")
    assert db.get_cutoff_snapshot(PROVIDER_ID).id == full_id

    partial_one = _complete_snapshot(db, "partial")
    partial_two = _complete_snapshot(db, "partial")

    cutoff = db.get_cutoff_snapshot(PROVIDER_ID)
    assert cutoff is not None
    assert cutoff.id == partial_two
    db.close()


def test_cutoff_skips_canceled_and_failed_snapshots():
    db = _make_database()
    _complete_snapshot(db, "full")
    partial_id = _complete_snapshot(db, "partial")
    _complete_snapshot(db, "canceled")
    _complete_snapshot(db, "failed")

    cutoff = db.get_cutoff_snapshot(PROVIDER_ID)
    assert cutoff is not None
    assert cutoff.id == partial_id
    db.close()
