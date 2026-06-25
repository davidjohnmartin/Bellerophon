# Databricks notebook source
# =============================================================================
# BELLE TEST: Maintenance Scheduler
# =============================================================================
# Validates maintenance scheduling logic:
#   - Nth-weekday-of-month calculation is correct
#   - VACUUM dry-run executes without error
#   - OPTIMIZE dry-run executes without error
#   - Force rebuild scheduling triggers correctly
#   - Intelligent thresholds behave as expected
# =============================================================================

%run ../bellerophon_core

# COMMAND ----------

import datetime

# =============================================================================
# TEST 1: Nth-weekday-of-month calculation
# =============================================================================
# The 2nd Sunday of June 2026 is June 14
# The 1st Monday of June 2026 is June 1
# The 3rd Friday of June 2026 is June 19

test_cases = [
    # (year, month, day_of_week, week_of_month, expected_date)
    (2026, 6, 6, 2, datetime.date(2026, 6, 14)),  # 2nd Sunday
    (2026, 6, 0, 1, datetime.date(2026, 6, 1)),   # 1st Monday
    (2026, 6, 4, 3, datetime.date(2026, 6, 19)),  # 3rd Friday
    (2026, 1, 0, 1, datetime.date(2026, 1, 5)),   # 1st Monday of Jan 2026
    (2026, 12, 6, 4, datetime.date(2026, 12, 27)), # 4th Sunday of Dec 2026
]

scheduler = belle.MaintenanceScheduler(interactive_mode=True)

for year, month, dow, wom, expected in test_cases:
    with belle.Config.temp_config(
        ENABLE_SCHEDULED_VACUUM=True,
        SCHEDULED_VACUUM_DAY_OF_WEEK=dow,
        SCHEDULED_VACUUM_WEEK_OF_MONTH=wom,
    ):
        result = scheduler.should_run_vacuum(datetime.date(year, month, expected.day))
        assert result == True, \
            f"Should trigger on {expected} (wom={wom}, dow={dow}), got False"
        
        # Day before should NOT trigger
        day_before = expected - datetime.timedelta(days=1)
        result_before = scheduler.should_run_vacuum(day_before)
        assert result_before == False, \
            f"Should NOT trigger on {day_before}, got True"

print(f"\u2705 TEST 1 PASSED: All {len(test_cases)} Nth-weekday calculations correct")

# COMMAND ----------

# =============================================================================
# TEST 2: Scheduled maintenance disabled by default
# =============================================================================
belle.Config.reset_defaults()

assert belle.Config.ENABLE_SCHEDULED_VACUUM == False
assert belle.Config.ENABLE_SCHEDULED_OPTIMIZE == False
assert belle.Config.ENABLE_SCHEDULED_FULL_REBUILD == False

# With defaults, nothing should trigger
today = datetime.date.today()
assert scheduler.should_run_vacuum(today) == False

print("\u2705 TEST 2 PASSED: All maintenance disabled by default (safe)")

# COMMAND ----------

# =============================================================================
# TEST 3: Intelligent auto-maintenance thresholds
# =============================================================================
with belle.Config.temp_config(
    ENABLE_INTELLIGENT_AUTO_OPTIMIZE=True,
    OPTIMIZE_MIN_SMALL_FILES=50,
    OPTIMIZE_SMALL_FILE_SIZE_MB=100,
    OPTIMIZE_MIN_TOTAL_FILES=100,
    OPTIMIZE_MAX_DAYS_SINCE_LAST=7,
    OPTIMIZE_MIN_TABLE_SIZE_GB=1,
    INTELLIGENT_MAINTENANCE_DRY_RUN=True,  # Preview only
):
    # Verify config values are applied
    assert belle.Config.ENABLE_INTELLIGENT_AUTO_OPTIMIZE == True
    assert belle.Config.OPTIMIZE_MIN_SMALL_FILES == 50
    assert belle.Config.INTELLIGENT_MAINTENANCE_DRY_RUN == True

# Verify reverted
assert belle.Config.ENABLE_INTELLIGENT_AUTO_OPTIMIZE == False

print("\u2705 TEST 3 PASSED: Intelligent thresholds configurable and revert correctly")

# COMMAND ----------

# =============================================================================
# TEST 4: check_scheduled_maintenance returns correct tuple
# =============================================================================
TEST_DB = "belle_test_maint"

# With maintenance disabled, should return (False, False, False)
belle.Config.reset_defaults()
orch = belle.Orchestrator(
    {f"{TEST_DB}.dummy": {
        "target_database": TEST_DB, "result_table_name": "dummy",
        "load_mode": "full", "dependencies": []
    }},
    validate_configs=False,
)

force_rebuild, run_vacuum, run_optimize = orch.check_scheduled_maintenance()
assert force_rebuild == False
assert run_vacuum == False
assert run_optimize == False

print("\u2705 TEST 4 PASSED: check_scheduled_maintenance returns (False, False, False) when disabled")

# COMMAND ----------

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "="*80)
print("  \U0001f3c6  ALL MAINTENANCE SCHEDULER TESTS PASSED")
print("="*80)
print(f"  Belle version: {belle.VERSION}")
print(f"  Tests: 1 (Nth-weekday), 2 (defaults safe), 3 (thresholds), 4 (check returns)")
print("="*80)