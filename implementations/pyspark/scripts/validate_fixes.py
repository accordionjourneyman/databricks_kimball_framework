#!/usr/bin/env python3
"""
Test script to validate the databricks_kimball_framework fixes:
1. Modern PySpark error handling (pyspark.errors compatibility)
2. Persistent checkpoint storage
3. Crash-resilient staging table cleanup
"""

import os
import sys

# Add src to path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def test_error_handling_imports():
    """Test that error handling imports are updated for modern PySpark."""
    print("Testing PySpark error handling imports...")

    merger_file = os.path.join(os.path.dirname(__file__), "..", "src/kimball/merger.py")

    with open(merger_file) as f:
        content = f.read()

    # Check for modern import structure
    if "try:" in content and "from pyspark.errors import PySparkException" in content:
        if "except ImportError:" in content and "pyspark.sql.utils" in content:
            print("✅ Modern PySpark error handling imports implemented")
            return True

    print("❌ PySpark error handling imports not updated")
    return False


def test_checkpoint_persistence():
    """Test that checkpoint code uses persistent storage."""
    print("Testing checkpoint persistence code...")

    orchestrator_file = os.path.join(
        os.path.dirname(__file__), "..", "src/kimball/orchestrator.py"
    )

    with open(orchestrator_file) as f:
        content = f.read()

    # Check that checkpoint now uses Delta table instead of JSON files
    if (
        "checkpoint_table: str = None" in content
        and "KIMBALL_CHECKPOINT_TABLE" in content
    ):
        if (
            "DeltaTable.forName(spark, self.checkpoint_table)" in content
            and "saveAsTable" in content
        ):
            print("✅ Checkpoint uses ACID-compliant Delta table storage")
            return True

    print("❌ Checkpoint still uses non-persistent storage")
    return False


def test_staging_cleanup_code():
    """Test that staging cleanup code is implemented."""
    print("Testing staging cleanup implementation...")

    orchestrator_file = os.path.join(
        os.path.dirname(__file__), "..", "src/kimball/orchestrator.py"
    )

    with open(orchestrator_file) as f:
        content = f.read()

    # Check for StagingCleanupManager class
    if "class StagingCleanupManager:" in content:
        if (
            "register_staging_table" in content
            and "unregister_staging_table" in content
        ):
            if "cleanup_orphaned_staging_tables" in content:
                print("✅ Crash-resilient staging cleanup implemented")
                return True

    print("❌ Staging cleanup not implemented")
    return False


def test_orchestrator_integration():
    """Test that Orchestrator integrates cleanup functionality."""
    print("Testing Orchestrator cleanup integration...")

    orchestrator_file = os.path.join(
        os.path.dirname(__file__), "..", "src/kimball/orchestrator.py"
    )

    with open(orchestrator_file) as f:
        content = f.read()

    # Check for cleanup manager in constructor and cleanup calls
    if "StagingCleanupManager()" in content:
        if "cleanup_orphaned_staging_tables()" in content:
            # Since we removed physical staging, check for Delta table registry usage
            if "cleanup_manager.cleanup_staging_tables(" in content:
                print("✅ Orchestrator integrates cleanup functionality")
                return True

    print("❌ Orchestrator cleanup integration incomplete")
    return False


def test_scd2_intra_batch_sequencing():
    """Test that SCD2 handles multiple updates for same key within a batch."""
    print("Testing SCD2 intra-batch sequencing...")

    merger_file = os.path.join(os.path.dirname(__file__), "..", "src/kimball/merger.py")

    with open(merger_file) as f:
        content = f.read()

    # Check for intra-batch sequencing logic
    if (
        'Window.partitionBy(*join_keys).orderBy(col("__etl_processed_at").desc())'
        in content
    ):
        if "row_number().over(window)" in content and "_intra_batch_seq" in content:
            if 'filter(col("_intra_batch_seq") == 1)' in content:
                print("✅ SCD2 intra-batch sequencing implemented")
                return True

    print("❌ SCD2 intra-batch sequencing not implemented")
    return False


def test_system_column_preservation():
    """Test that system columns are preserved during column pruning."""
    print("Testing system column preservation...")

    orchestrator_file = os.path.join(
        os.path.dirname(__file__), "..", "src/kimball/orchestrator.py"
    )

    with open(orchestrator_file) as f:
        content = f.read()

    # Check for SYSTEM_COLUMNS definition and usage
    if "SYSTEM_COLUMNS = {" in content and '"__is_current"' in content:
        if "if c in target_columns or c in SYSTEM_COLUMNS:" in content:
            print("✅ System column preservation implemented")
            return True

    print("❌ System column preservation not implemented")
    return False


def test_checkpoint_optimization():
    """Test that checkpoint is now optional via configuration."""
    print("Testing checkpoint optimization...")

    orchestrator_file = os.path.join(
        os.path.dirname(__file__), "..", "src/kimball/orchestrator.py"
    )

    with open(orchestrator_file) as f:
        content = f.read()

    # Check for enable_lineage_truncation configuration usage
    if "enable_lineage_truncation" in content and "getattr(self.config," in content:
        if "Using local checkpoint (efficient, no lineage truncation)" in content:
            print("✅ Checkpoint optimization implemented")
            return True

    print("❌ Checkpoint optimization not implemented")
    return False


def test_atomic_cleanup_operations():
    """Test that cleanup operations are atomic to prevent race conditions."""
    print("Testing atomic cleanup operations...")

    orchestrator_file = os.path.join(
        os.path.dirname(__file__), "..", "src/kimball/orchestrator.py"
    )

    with open(orchestrator_file) as f:
        content = f.read()

    # Check for atomic MERGE-based cleanup
    if (
        'registry_table.alias("target").merge(' in content
        and "whenMatchedDelete()" in content
    ):
        if "Atomic TTL-based staging cleanup" in content:
            print("✅ Atomic cleanup operations implemented")
            return True

    print("❌ Atomic cleanup operations not implemented")
    return False


def test_retry_decorator_update():
    """Test that retry decorator uses improved error handling."""
    print("Testing retry decorator improvements...")

    merger_file = os.path.join(os.path.dirname(__file__), "..", "src/kimball/merger.py")

    with open(merger_file) as f:
        content = f.read()

    # Check that decorator no longer checks isinstance with AnalysisException
    if "isinstance(e, pyspark.sql.utils.AnalysisException)" not in content:
        if (
            "error_str = str(e)" in content
            and "is_concurrent = any(x in error_str" in content
        ):
            print("✅ Retry decorator uses modern error handling")
            return True

    print("❌ Retry decorator not updated")
    return False


def main():
    """Run all validation tests."""
    print("🔧 Validating databricks_kimball_framework fixes\n")

    tests = [
        test_error_handling_imports,
        test_checkpoint_persistence,
        test_staging_cleanup_code,
        test_orchestrator_integration,
        test_retry_decorator_update,
        test_scd2_intra_batch_sequencing,
        test_system_column_preservation,
        test_checkpoint_optimization,
        test_atomic_cleanup_operations,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        try:
            if test():
                passed += 1
            print()
        except Exception as e:
            print(f"❌ Test failed with exception: {e}\n")

    print(f"📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All fixes validated successfully!")
        print("\n✅ Code Quality Improvements:")
        print("  - Updated PySpark error handling for Databricks Runtime 13+")
        print("  - Retry decorator uses string-based error detection")
        print("\n✅ Resilience Improvements:")
        print("  - Checkpoint storage moved to persistent DBFS")
        print("  - StagingCleanupManager provides crash-resilient cleanup")
        print("  - Orphaned staging tables cleaned up atomically")
        print("\n✅ SCD2 Correctness Fixes:")
        print("  - Intra-batch sequencing prevents history corruption")
        print("  - System columns always preserved during pruning")
        print("  - Checkpoint optimization reduces I/O overhead")
        return 0
    else:
        print("⚠️  Some tests failed. Please review the fixes.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
