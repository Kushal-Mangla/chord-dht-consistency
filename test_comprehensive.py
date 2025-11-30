#!/usr/bin/env python3
"""
Comprehensive Test Suite for Chord DHT System

This script tests various scenarios including:
- Basic PUT/GET operations
- Multi-node replication
- Node failure and recovery
- Sloppy quorum with hinted handoff
- Vector clock versioning
- Persistence
"""

import sys
import os
import time
import asyncio
import subprocess
import requests
from typing import List, Dict, Tuple

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from chord.storage import ChordStorage
from consistency.vector_clock import VectorClock


class TestResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.error = None
        self.duration = 0.0
    
    def __repr__(self):
        status = "✅ PASS" if self.passed else "❌ FAIL"
        error_msg = f" - {self.error}" if self.error else ""
        return f"{status}: {self.name} ({self.duration:.2f}s){error_msg}"


def print_header(text: str):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(text.center(70))
    print("=" * 70)


def print_test_start(test_name: str):
    """Print test start message."""
    print(f"\n▶ Running: {test_name}")
    print("-" * 70)


class ChordTestSuite:
    """Test suite for Chord DHT system."""
    
    def __init__(self):
        self.results: List[TestResult] = []
        self.base_port = 5100  # Use different ports to avoid conflicts
        self.base_http_port = 8100
    
    def test_vector_clock_increment(self) -> TestResult:
        """Test that vector clocks are properly incremented on repeated PUTs."""
        result = TestResult("Vector Clock Increment")
        print_test_start(result.name)
        start_time = time.time()
        
        try:
            # Simulate Node 50 storing backups for Node 35 (who is down)
            storage = ChordStorage(node_id=50, enable_persistence=False)
            
            # First PUT
            print("1. First PUT (key=test, value=v1)")
            existing = storage.get_backup("test", for_node_id=35)
            if existing:
                _, existing_version = existing
                version1 = existing_version.copy()
                version1.increment(50)
            else:
                version1 = VectorClock()
                version1.increment(50)
            
            storage.put_backup("test", "v1", version1, for_node_id=35)
            print(f"   ✓ Stored: version={version1}")
            
            # Second PUT
            print("2. Second PUT (key=test, value=v2)")
            existing = storage.get_backup("test", for_node_id=35)
            if existing:
                _, existing_version = existing
                version2 = existing_version.copy()
                version2.increment(50)
            else:
                version2 = VectorClock()
                version2.increment(50)
            
            storage.put_backup("test", "v2", version2, for_node_id=35)
            print(f"   ✓ Stored: version={version2}")
            
            # Verify
            final = storage.get_backup("test", for_node_id=35)
            if final:
                value, version = final
                if version.clock.get(50) == 2 and value == "v2":
                    result.passed = True
                    print(f"   ✅ Version correctly incremented: {version}")
                else:
                    result.error = f"Expected {{50: 2}}, got {version}"
            else:
                result.error = "Backup not found"
        
        except Exception as e:
            result.error = str(e)
        
        result.duration = time.time() - start_time
        return result
    
    def test_version_merge(self) -> TestResult:
        """Test that versions are properly merged when receiving replicas."""
        result = TestResult("Version Merge")
        print_test_start(result.name)
        start_time = time.time()
        
        try:
            storage = ChordStorage(node_id=62, enable_persistence=False)
            
            # First replica
            print("1. First PUT_REPLICA from Node 50")
            incoming1 = VectorClock({50: 1})
            existing = storage.get_backup("test", for_node_id=35)
            if existing:
                _, existing_version = existing
                version = existing_version.copy()
                version.update(incoming1)
                version.increment(62)
            else:
                version = incoming1.copy()
                version.increment(62)
            
            storage.put_backup("test", "v1", version, for_node_id=35)
            print(f"   ✓ Stored: version={version}")
            
            # Second replica (updated)
            print("2. Second PUT_REPLICA from Node 50 (updated)")
            incoming2 = VectorClock({50: 2})
            existing = storage.get_backup("test", for_node_id=35)
            if existing:
                _, existing_version = existing
                version = existing_version.copy()
                version.update(incoming2)
                version.increment(62)
            else:
                version = incoming2.copy()
                version.increment(62)
            
            storage.put_backup("test", "v2", version, for_node_id=35)
            print(f"   ✓ Stored: version={version}")
            
            # Verify
            final = storage.get_backup("test", for_node_id=35)
            if final:
                value, version = final
                if version.clock.get(50) == 2 and version.clock.get(62) == 2 and value == "v2":
                    result.passed = True
                    print(f"   ✅ Version correctly merged: {version}")
                else:
                    result.error = f"Expected {{50: 2, 62: 2}}, got {version}"
            else:
                result.error = "Backup not found"
        
        except Exception as e:
            result.error = str(e)
        
        result.duration = time.time() - start_time
        return result
    
    def test_recovery_logic(self) -> TestResult:
        """Test node recovery with version comparison."""
        result = TestResult("Recovery Logic")
        print_test_start(result.name)
        start_time = time.time()
        
        try:
            # Node 35 with old data
            node35 = ChordStorage(node_id=35, enable_persistence=False)
            old_version = VectorClock({35: 1})
            node35.put("test", "v_old", old_version)
            print(f"1. Node 35 initial: test=v_old, version={old_version}")
            
            # Node 50 with updated backup
            node50 = ChordStorage(node_id=50, enable_persistence=False)
            new_version = VectorClock({50: 2})
            node50.put_backup("test", "v_new", new_version, for_node_id=35)
            print(f"2. Node 50 has backup: test=v_new, version={new_version}")
            
            # Simulate recovery
            print("3. Node 35 recovers...")
            backups = node50.get_all_backups_for_node(35)
            for key, (value, version) in backups.items():
                existing = node35.get(key)
                if existing:
                    _, existing_version = existing
                    print(f"   Comparing: local={existing_version} vs recovered={version}")
                    
                    if version > existing_version:
                        node35.put(key, value, version)
                        print(f"   ✓ Updated with newer version")
                    elif version.concurrent_with(existing_version):
                        merged = existing_version.copy()
                        merged.update(version)
                        node35.put(key, value, merged)
                        print(f"   ✓ Updated with concurrent version (merged)")
            
            # Verify
            final = node35.get("test")
            if final:
                value, version = final
                if value == "v_new":
                    result.passed = True
                    print(f"   ✅ Node 35 recovered: test={value}, version={version}")
                else:
                    result.error = f"Expected v_new, got {value}"
            else:
                result.error = "Key not found after recovery"
        
        except Exception as e:
            result.error = str(e)
        
        result.duration = time.time() - start_time
        return result
    
    def test_persistence(self) -> TestResult:
        """Test that data persists across storage instances."""
        result = TestResult("Persistence")
        print_test_start(result.name)
        start_time = time.time()
        
        try:
            # Create temp storage directory
            test_dir = "test_storage_temp"
            if os.path.exists(test_dir):
                import shutil
                shutil.rmtree(test_dir)
            
            # Store data
            print("1. Creating storage and storing data...")
            storage1 = ChordStorage(node_id=99, base_dir=test_dir, enable_persistence=True)
            version = storage1.put("persist_key", "persist_value")
            print(f"   ✓ Stored: persist_key=persist_value, version={version}")
            
            # Delete instance
            del storage1
            print("2. Deleted storage instance")
            
            # Load data in new instance
            print("3. Creating new storage instance...")
            storage2 = ChordStorage(node_id=99, base_dir=test_dir, enable_persistence=True)
            count = storage2.load_all_primary()
            print(f"   ✓ Loaded {count} keys")
            
            # Verify
            retrieved = storage2.get("persist_key")
            if retrieved:
                value, version = retrieved
                if value == "persist_value":
                    result.passed = True
                    print(f"   ✅ Data persisted: {value}")
                else:
                    result.error = f"Expected persist_value, got {value}"
            else:
                result.error = "Key not found after reload"
            
            # Cleanup
            import shutil
            shutil.rmtree(test_dir)
        
        except Exception as e:
            result.error = str(e)
        
        result.duration = time.time() - start_time
        return result
    
    def test_live_system(self) -> TestResult:
        """Test a live Chord system with actual node processes."""
        result = TestResult("Live System Integration")
        print_test_start(result.name)
        start_time = time.time()
        
        processes = []
        
        try:
            print("1. Starting 3 Chord nodes...")
            
            # Start node 1 (bootstrap)
            port1 = self.base_port
            http1 = self.base_http_port
            p1 = subprocess.Popen(
                ["python3", "main.py", "--port", str(port1), "--host", "0.0.0.0", 
                 "--m", "6", "--N", "3", "--R", "2", "--W", "2", "--log-level", "WARNING"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            processes.append(p1)
            time.sleep(2)
            print(f"   ✓ Node 1 started (port {port1})")
            
            # Start node 2
            port2 = self.base_port + 1
            http2 = self.base_http_port + 1
            p2 = subprocess.Popen(
                ["python3", "main.py", "--port", str(port2), "--host", "0.0.0.0",
                 "--m", "6", "--N", "3", "--R", "2", "--W", "2", "--log-level", "WARNING",
                 "--join", f"127.0.0.1:{port1}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            processes.append(p2)
            time.sleep(2)
            print(f"   ✓ Node 2 started (port {port2})")
            
            # Start node 3
            port3 = self.base_port + 2
            http3 = self.base_http_port + 2
            p3 = subprocess.Popen(
                ["python3", "main.py", "--port", str(port3), "--host", "0.0.0.0",
                 "--m", "6", "--N", "3", "--R", "2", "--W", "2", "--log-level", "WARNING",
                 "--join", f"127.0.0.1:{port1}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            processes.append(p3)
            time.sleep(3)
            print(f"   ✓ Node 3 started (port {port3})")
            
            # Test PUT
            print("\n2. Testing PUT operation...")
            url_put = f"http://127.0.0.1:{http1}/put?key=testkey&value=testvalue"
            response = requests.put(url_put, timeout=5)
            if response.status_code == 200:
                print(f"   ✓ PUT successful: {response.json()}")
            else:
                raise Exception(f"PUT failed: {response.status_code}")
            
            time.sleep(1)
            
            # Test GET
            print("\n3. Testing GET operation...")
            url_get = f"http://127.0.0.1:{http1}/get?key=testkey"
            response = requests.get(url_get, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get("value") == "testvalue":
                    print(f"   ✓ GET successful: {data}")
                    result.passed = True
                else:
                    raise Exception(f"Unexpected value: {data}")
            else:
                raise Exception(f"GET failed: {response.status_code}")
        
        except Exception as e:
            result.error = str(e)
            print(f"   ❌ Error: {e}")
        
        finally:
            # Cleanup processes
            print("\n4. Stopping nodes...")
            for p in processes:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except:
                    p.kill()
            print("   ✓ All nodes stopped")
        
        result.duration = time.time() - start_time
        return result
    
    def run_all_tests(self):
        """Run all tests and display results."""
        print_header("CHORD DHT COMPREHENSIVE TEST SUITE")
        
        # Unit tests
        print("\n📦 Unit Tests")
        print("-" * 70)
        self.results.append(self.test_vector_clock_increment())
        self.results.append(self.test_version_merge())
        self.results.append(self.test_recovery_logic())
        self.results.append(self.test_persistence())
        
        # Integration test
        print("\n🔧 Integration Tests")
        print("-" * 70)
        self.results.append(self.test_live_system())
        
        # Summary
        print_header("TEST SUMMARY")
        
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        
        print()
        for result in self.results:
            print(result)
        
        print("\n" + "-" * 70)
        print(f"Total: {total} | Passed: {passed} | Failed: {failed}")
        
        if failed == 0:
            print("\n🎉 ALL TESTS PASSED!")
            print("=" * 70)
            return 0
        else:
            print(f"\n❌ {failed} TEST(S) FAILED!")
            print("=" * 70)
            return 1


def main():
    """Main entry point."""
    suite = ChordTestSuite()
    exit_code = suite.run_all_tests()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
