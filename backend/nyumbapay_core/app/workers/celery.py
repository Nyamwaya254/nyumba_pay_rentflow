"""Celery app and all scheduled + triggered tasks.
Beat schedule (all times EAT = UTC+3):
  - 1st of month 00:00 → generate_monthly_ledger (creates ledger rows)
  - 1st of month 08:00 → send_reminder_1st (unpaid from PREVIOUS month)
  - 28th of month 08:00 → send_reminder_28 (current month due)
  - 5th of month 08:00 → send_reminder_5th (final escalation)
  - Continuous        → consume_payment_event (Redis Pub/Sub listener)
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
import uuid
from zoneinfo import ZoneInfo
from celery import Celery
from celery.schedules import crontab
import structlog
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool

from nyumbapay_core.app.core.config import get_settings
from nyumbapay_core.app.models.models import BuildingChargeConfig
from nyumbapay_core.app.repositories.repos import (
    BuildingRepository,
    LeaseRepository,
    LedgerRepository,
)


logger = structlog.get_logger(__name__)

settings = get_settings()

celery_app = Celery(
    "nyumbapay",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Africa/Nairobi",
    enable_utc=True,
    task_acks_late=True,  # re-queue of worker crash
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,  # Fair dispatch for long tasks
    beat_scheduler="redbeat.RedBeatScheduler",  # Redbeat- persistent schedule storage in Redis
    readbeat_redis_url=settings.redis_url_str,  # redis url Redbeat uses to store schedule state
    redbeat_lock_timeout=settings.beat_lock_timeout,  # distributed lock TTL in seconds
    redbeat_key_prefix="nyumbapay:beat",
    task_routes={
        "app.workers.celery.generate_monthly_ledger": {"queue": "ledger"},
        "app.workers.celery.send_reminder_28": {"queue": "notifications"},
        "app.workers.celery.send_reminder_1st": {"queue": "notifications"},
        "app.workers.celery.send_reminder_5th": {"queue": "notifications"},
        "app.workers.celery.send_payment_confirmation_task": {"queue": "notifications"},
    },
    beat_schedule={
        # Ledger row generation — runs at midnight on the 1st
        "generate-monthly-ledger": {
            "task": "app.workers.celery.generate_monthly_ledger",
            "schedule": crontab(hour=0, minute=0, day_of_month=1),
        },
        # 28th reminder — fire after landlord enters water readings
        "reminder-28th": {
            "task": "app.workers.celery.send_reminder_28",
            "schedule": crontab(hour=8, minute=0, day_of_month=28),
        },
        # 1st follow-up — only to still-unpaid tenants from PREVIOUS month
        "reminder-1st": {
            "task": "app.workers.celery.send_reminder_1st",
            "schedule": crontab(hour=8, minute=0, day_of_month=1),
        },
        # 5th final escalation for rent reminder
        "reminder-5th": {
            "task": "app.workers.celery.send_reminder_5th",
            "schedule": crontab(hour=8, minute=0, day_of_month=5),
        },
    },
)


# helpers
def _run(coro):
    """Execute an async coroutine from a synchronous Celery task body"""
    return asyncio.run(coro)


def _prev_period(period: str) -> str:
    """Return the previous YYYY-MM period string."""
    year, month = int(period[:4]), int(period[5:])
    if month == 1:
        return f"{year - 1}-12"
    return f"{year}-{month - 1:02d}"


@asynccontextmanager
async def _get_db_session():
    """Async context manager that yields a transaction-wrapped database session"""
    cfg = get_settings()
    engine = create_async_engine(cfg.database_url_str, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with factory() as session:
            async with session.begin():
                yield session
    finally:
        # always run-clean exit or expection
        await engine.dispose()


# task: Generate Monthly ledger


@celery_app.task(
    name="app.workers.celery.generate_montly_ledger",
    bind=True,
    max_retries=3,
    default_retry_delay=300,
)
def generate_montly_ledger(self):
    """Create rent_ledger rows for all active leases for a new month
    -Runs at midnight on the !st.Set previous balance at 0,carry forward balance is calculated dynamically when 28th reminder is fired.
    -Wate charge starts at 0 -updated when landlord enters readings btwn 1-20
    """

    async def _run_async():
        async with _get_db_session() as session:
            lease_repo = LeaseRepository(session)
            ledger_repo = LedgerRepository(session)
            building_repo = BuildingRepository(session)

            # all active leases + their units in one join
            leases = await lease_repo.get_all_active_with_units()
            # latest charge config for every building in one CTE query
            charge_configs = await building_repo.get_all_latest_charge_configs()
            config_map: dict[uuid.UUID, BuildingChargeConfig] = {
                cfg.building_id: cfg for cfg in charge_configs
            }

            EAT = ZoneInfo("Africa/Nairobi")
            now_eat = datetime.now(tz=EAT)
            period = f"{now_eat.year}-{now_eat.month:02d}"
            created = 0
            skipped = 0

            for lease in leases:
                try:
                    async with session.begin_nested():
                        # only check current-period since prev_period no longer needed
                        current_entry = await ledger_repo.get_by_lease_period(
                            lease.id, period
                        )
                        if current_entry:
                            skipped += 1
                            continue

                        cfg_entry = config_map.get(lease.unit.building_id)
                        if not cfg_entry:
                            logger.warning(
                                "no_charge_config_for_building",
                                building_id=str(lease.unit.building_id),
                                lease_id=str(lease.id),
                            )
                            continue
                        # always zero - carry-forward handled by apply_payment()
                        await ledger_repo.create(
                            lease_id=lease.id,
                            period=period,
                            base_rent=lease.rent_amount,
                            garbage_charge=cfg_entry.garbage_charge,
                            previous_balance=Decimal("0"),
                        )
                        created += 1
                except Exception as exc:
                    logger.error(
                        "ledger_generation_failed_for_lease",
                        lease_id=str(lease.id),
                        error=str(exc),
                    )
            logger.info(
                "montly_ledger_generated",
                period=period,
                created=created,
                skipped=skipped,
            )
            return {"period": period, "created": created, "skipped": skipped}

    try:
        return _run(_run_async())
    except Exception as exc:
        logger.error("generate_monthly_ledger_failed", error=str(exc))
        raise self.retry(exc=exc)
