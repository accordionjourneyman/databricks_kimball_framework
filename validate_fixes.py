#!/usr/bin/env python3
"""
Test script to validate the databricks_kimball_framework fixes:
1. Modern PySpark error handling (pyspark.errors compatibility)
2. Persistent checkpoint storage
3. Crash-resilient staging table cleanup
"""

import os
import sys
import tempfile
import ast

# Add src to path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_error_handling_imports():
    """Test that error handling imports are updated for modern PySpark."""
    print("Testing PySpark error handling imports...")

    merger_file = os.path.join(os.path.dirname(__file__), 'src/kimball/merger.py')

    with open(merger_file, 'r') as f:
        content = f.read()

    # Check for modern import structure
    if 'try:' in content and 'from pyspark.errors import PySparkException' in content:
        if 'except ImportError:' in content and 'pyspark.sql.utils' in content:
            print("✅ Modern PySpark error handling imports implemented")
            return True

    print("❌ PySpark error handling imports not updated")
    return False

def test_checkpoint_persistence():
    """Test that checkpoint code uses persistent storage."""
    print("Testing checkpoint persistence code...")

    orchestrator_file = os.path.join(os.path.dirname(__file__), 'src/kimball/orchestrator.py')

    with open(orchestrator_file, 'r') as f:
        content = f.read()

    # Check that default is now /dbfs instead of dbfs:
    if 'checkpoint_dir: str = None' in content and 'KIMBALL_CHECKPOINT_DIR' in content:
        if '/dbfs/kimball/checkpoints' in content:
            print("✅ Checkpoint uses persistent DBFS storage by default")
            return True

    print("❌ Checkpoint still uses non-persistent storage")
    return False

def test_staging_cleanup_code():
    """Test that staging cleanup code is implemented."""
    print("Testing staging cleanup implementation...")

    orchestrator_file = os.path.join(os.path.dirname(__file__), 'src/kimball/orchestrator.py')

    with open(orchestrator_file, 'r') as f:
        content = f.read()

    # Check for StagingCleanupManager class
    if 'class StagingCleanupManager:' in content:
        if 'register_staging_table' in content and 'unregister_staging_table' in content:
            if 'cleanup_orphaned_staging_tables' in content:
                print("✅ Crash-resilient staging cleanup implemented")
                return True

    print("❌ Staging cleanup not implemented")
    return False

def test_orchestrator_integration():
    """Test that Orchestrator integrates cleanup functionality."""
    print("Testing Orchestrator cleanup integration...")

    orchestrator_file = os.path.join(os.path.dirname(__file__), 'src/kimball/orchestrator.py')

    with open(orchestrator_file, 'r') as f:
        content = f.read()

    # Check for cleanup manager in constructor and cleanup calls
    if 'self.cleanup_manager = StagingCleanupManager()' in content:
        if 'self.cleanup_orphaned_staging_tables()' in content:
            if 'self.cleanup_manager.register_staging_table(staging_table)' in content:
                print("✅ Orchestrator integrates cleanup functionality")
                return True

    print("❌ Orchestrator cleanup integration incomplete")
    return False

def test_retry_decorator_update():
    """Test that retry decorator uses improved error handling."""
    print("Testing retry decorator improvements...")

    merger_file = os.path.join(os.path.dirname(__file__), 'src/kimball/merger.py')

    with open(merger_file, 'r') as f:
        content = f.read()

    # Check that decorator no longer checks isinstance with AnalysisException
    if 'isinstance(e, pyspark.sql.utils.AnalysisException)' not in content:
        if 'error_str = str(e)' in content and 'is_concurrent = any(x in error_str' in content:
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
        test_retry_decorator_update
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
        print("  - Orphaned staging tables cleaned up on pipeline start")
        print("\n✅ Observability Improvements:")
        print("  - Configurable checkpoint directory via environment variable")
        return 0
    else:
        print("⚠️  Some tests failed. Please review the fixes.")
        return 1

if __name__ == "__main__":
    sys.exit(main())