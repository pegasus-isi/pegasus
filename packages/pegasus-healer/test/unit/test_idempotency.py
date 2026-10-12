"""
AC-01: Duplicate events must never create duplicate incidents.

These tests use an in-memory SQLite-backed session via SQLAlchemy
so no live PostgreSQL is required for unit testing.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.db.models import Base
from app.events.persistence import get_or_create_incident, persist_event
from app.models.events import WorkflowEvent


@pytest.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        yield s
    await engine.dispose()


def _event(job_instance_id: int = 1) -> WorkflowEvent:
    return WorkflowEvent(
        event_id=WorkflowEvent.make_event_id({"wf": "wf-001", "inst": job_instance_id}),
        event_type="JOB_FAILED",
        workflow_id="wf-001",
        job_id="bowtie_0",
        job_instance_id=job_instance_id,
        status=137,
        timestamp=datetime.now(timezone.utc),
    )


@pytest.mark.asyncio
async def test_duplicate_event_returns_false(session: AsyncSession):
    """AC-01: Same event_id inserted twice → second insert returns False."""
    event = _event()
    first = await persist_event(session, event)
    await session.commit()

    second = await persist_event(session, event)
    await session.commit()

    assert first is True
    assert second is False  # duplicate silently rejected


@pytest.mark.asyncio
async def test_duplicate_incident_returns_existing(session: AsyncSession):
    """AC-01: Same (workflow_id, job_id, job_instance_id) → same incident returned."""
    event = _event()
    incident1, created1 = await get_or_create_incident(session, event)
    await session.commit()

    incident2, created2 = await get_or_create_incident(session, event)
    await session.commit()

    assert created1 is True
    assert created2 is False
    assert incident1.id == incident2.id


@pytest.mark.asyncio
async def test_different_instance_creates_new_incident(session: AsyncSession):
    """Different job_instance_id → new incident."""
    e1 = _event(job_instance_id=1)
    e2 = _event(job_instance_id=2)

    # Need unique event_ids
    e2 = e2.model_copy(update={"event_id": WorkflowEvent.make_event_id({"wf": "x", "i": 2})})

    inc1, _ = await get_or_create_incident(session, e1)
    await session.commit()
    inc2, created = await get_or_create_incident(session, e2)
    await session.commit()

    assert created is True
    assert inc1.id != inc2.id
