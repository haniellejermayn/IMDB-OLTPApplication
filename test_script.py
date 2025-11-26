#!/usr/bin/env python3
"""
MCO2 Test Script - Distributed Database Testing
Runs all concurrency and recovery test cases 3 times each and logs results.

Usage:
    python test_script.py [--base-url http://localhost:5000]
"""

import requests
import json
import time
import sys
from datetime import datetime

# Configuration
BASE_URL = "http://localhost:5000"
NUM_RUNS = 3
TEST_TCONST = "tt0000001"  # Make sure this exists in your database

# Results storage
results = {
    "test_run_info": {
        "timestamp": datetime.now().isoformat(),
        "base_url": BASE_URL,
        "runs_per_test": NUM_RUNS
    },
    "concurrency_tests": {},
    "recovery_tests": {},
    "summary": {}
}


def log(message, level="INFO"):
    """Print formatted log message"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def make_request(method, endpoint, data=None, params=None):
    """Make HTTP request and return response"""
    url = f"{BASE_URL}{endpoint}"
    try:
        if method == "GET":
            response = requests.get(url, params=params, timeout=30)
        elif method == "POST":
            response = requests.post(url, json=data, timeout=30)
        elif method == "PUT":
            response = requests.put(url, json=data, params=params, timeout=30)
        elif method == "DELETE":
            response = requests.delete(url, timeout=30)
        else:
            return {"error": f"Unknown method: {method}"}
        
        return {
            "status_code": response.status_code,
            "data": response.json() if response.content else {},
            "time_ms": response.elapsed.total_seconds() * 1000
        }
    except requests.exceptions.ConnectionError:
        return {"error": "Connection failed - is the backend running?"}
    except requests.exceptions.Timeout:
        return {"error": "Request timeout"}
    except Exception as e:
        return {"error": str(e)}


def check_health():
    """Check if all nodes are healthy"""
    log("Checking node health...")
    response = make_request("GET", "/health")
    
    if "error" in response:
        log(f"Health check failed: {response['error']}", "ERROR")
        return False
    
    health_data = response.get("data", {})
    all_healthy = True
    
    for node, info in health_data.items():
        status = "✓ ONLINE" if info.get("healthy") else "✗ OFFLINE"
        count = info.get("record_count", "N/A")
        log(f"  {node}: {status} ({count} records)")
        if not info.get("healthy"):
            all_healthy = False
    
    return all_healthy


def run_test(test_name, test_func, runs=NUM_RUNS):
    """Run a test multiple times and collect results"""
    log(f"\n{'='*60}")
    log(f"Running: {test_name}")
    log(f"{'='*60}")
    
    test_results = []
    successes = 0
    
    for i in range(runs):
        log(f"  Run {i+1}/{runs}...")
        start_time = time.time()
        
        try:
            result = test_func()
            elapsed = (time.time() - start_time) * 1000
            
            success = result.get("success", False) or "error" not in result
            if success:
                successes += 1
                log(f"    ✓ Passed ({elapsed:.0f}ms)")
            else:
                log(f"    ✗ Failed: {result.get('error', 'Unknown error')}", "WARN")
            
            test_results.append({
                "run": i + 1,
                "success": success,
                "elapsed_ms": elapsed,
                "result": result
            })
        except Exception as e:
            log(f"    ✗ Exception: {str(e)}", "ERROR")
            test_results.append({
                "run": i + 1,
                "success": False,
                "error": str(e)
            })
        
        # Small delay between runs
        time.sleep(0.5)
    
    summary = {
        "total_runs": runs,
        "successes": successes,
        "failures": runs - successes,
        "success_rate": f"{(successes/runs)*100:.0f}%"
    }
    
    log(f"  Summary: {successes}/{runs} passed ({summary['success_rate']})")
    
    return {
        "runs": test_results,
        "summary": summary
    }


# ==================== CONCURRENCY TEST CASES ====================

def test_case1_concurrent_reads():
    """Case 1: Concurrent reads from multiple nodes"""
    isolation_levels = ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]
    all_results = {}
    
    for level in isolation_levels:
        response = make_request("POST", "/test/concurrent-read", {
            "tconst": TEST_TCONST,
            "isolation_level": level
        })
        
        if "error" not in response:
            data = response.get("data", {})
            all_results[level] = {
                "consistent": data.get("consistent", False),
                "nodes_tested": data.get("nodes_tested", []),
                "time_ms": response.get("time_ms", 0)
            }
        else:
            all_results[level] = {"error": response["error"]}
    
    # Success if at least READ COMMITTED works and is consistent
    success = all_results.get("READ COMMITTED", {}).get("consistent", False)
    
    return {
        "success": success,
        "isolation_level_results": all_results
    }


def test_case2_read_write_conflict():
    """Case 2: One transaction writing while others read"""
    isolation_levels = ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]
    all_results = {}
    
    for level in isolation_levels:
        response = make_request("POST", "/test/read-write-conflict", {
            "tconst": TEST_TCONST,
            "new_data": {"runtime_minutes": 999},
            "isolation_level": level
        })
        
        if "error" not in response:
            data = response.get("data", {})
            all_results[level] = {
                "analysis": data.get("analysis", {}),
                "write_success": data.get("results", {}).get("write", {}).get("success", False),
                "time_ms": response.get("time_ms", 0)
            }
        else:
            all_results[level] = {"error": response["error"]}
    
    success = all_results.get("READ COMMITTED", {}).get("write_success", False)
    
    return {
        "success": success,
        "isolation_level_results": all_results
    }


def test_case3_concurrent_writes():
    """Case 3: Multiple concurrent write operations"""
    isolation_levels = ["READ COMMITTED", "SERIALIZABLE"]
    all_results = {}
    
    for level in isolation_levels:
        response = make_request("POST", "/test/concurrent-write", {
            "updates": [
                {"tconst": TEST_TCONST, "data": {"runtime_minutes": 100}},
                {"tconst": TEST_TCONST, "data": {"runtime_minutes": 110}},
                {"tconst": TEST_TCONST, "data": {"runtime_minutes": 120}}
            ],
            "isolation_level": level
        })
        
        if "error" not in response:
            data = response.get("data", {})
            results_data = data.get("results", {})
            successes = sum(1 for r in results_data.values() if r.get("success", False))
            
            all_results[level] = {
                "updates_attempted": data.get("updates_attempted", 0),
                "updates_succeeded": successes,
                "final_state": data.get("final_states", {}),
                "time_ms": response.get("time_ms", 0)
            }
        else:
            all_results[level] = {"error": response["error"]}
    
    success = all_results.get("READ COMMITTED", {}).get("updates_succeeded", 0) > 0
    
    return {
        "success": success,
        "isolation_level_results": all_results
    }


# ==================== RECOVERY TEST CASES ====================

def test_recovery_case1_fragment_to_central():
    """Case 1: Fragment to central replication failure simulation info"""
    response = make_request("POST", "/test/failure/fragment-to-central")
    
    if "error" not in response:
        data = response.get("data", {})
        return {
            "success": True,
            "scenario": data.get("scenario", ""),
            "steps": data.get("steps", []),
            "current_pending": data.get("current_pending", 0)
        }
    return {"success": False, "error": response.get("error")}


def test_recovery_case2_central_recovery():
    """Case 2: Central node recovery"""
    response = make_request("POST", "/test/failure/central-recovery")
    
    if "error" not in response:
        data = response.get("data", {})
        return {
            "success": True,
            "node": data.get("node", ""),
            "recovered": data.get("recovered", 0),
            "failed": data.get("failed", 0),
            "message": data.get("message", "")
        }
    return {"success": False, "error": response.get("error")}


def test_recovery_case3_central_to_fragment():
    """Case 3: Central to fragment replication failure simulation info"""
    response = make_request("POST", "/test/failure/central-to-fragment")
    
    if "error" not in response:
        data = response.get("data", {})
        return {
            "success": True,
            "scenario": data.get("scenario", ""),
            "steps": data.get("steps", []),
            "current_pending": data.get("current_pending", 0)
        }
    return {"success": False, "error": response.get("error")}


def test_recovery_case4_fragment_recovery():
    """Case 4: Fragment node recovery"""
    all_results = {}
    
    for node in ["node2", "node3"]:
        response = make_request("POST", "/test/failure/fragment-recovery", {"node": node})
        
        if "error" not in response:
            data = response.get("data", {})
            all_results[node] = {
                "recovered": data.get("recovered", 0),
                "failed": data.get("failed", 0),
                "message": data.get("message", "")
            }
        else:
            all_results[node] = {"error": response.get("error")}
    
    return {
        "success": True,
        "node_results": all_results
    }


def check_recovery_status():
    """Check pending replication status"""
    response = make_request("GET", "/recovery/status")
    
    if "error" not in response:
        data = response.get("data", {})
        return {
            "success": True,
            "total_pending": data.get("total_pending", 0),
            "by_node": data.get("by_node", {}),
            "auto_retry_active": data.get("automatic_retry_active", False)
        }
    return {"success": False, "error": response.get("error")}


# ==================== MAIN TEST RUNNER ====================

def main():
    global BASE_URL
    
    # Parse command line args
    if len(sys.argv) > 1:
        for i, arg in enumerate(sys.argv):
            if arg == "--base-url" and i + 1 < len(sys.argv):
                BASE_URL = sys.argv[i + 1]
    
    results["test_run_info"]["base_url"] = BASE_URL
    
    log("=" * 60)
    log("MCO2 DISTRIBUTED DATABASE TEST SCRIPT")
    log("=" * 60)
    log(f"Target: {BASE_URL}")
    log(f"Test runs per case: {NUM_RUNS}")
    log(f"Test tconst: {TEST_TCONST}")
    log("")
    
    # Health check
    if not check_health():
        log("Not all nodes are healthy. Some tests may fail.", "WARN")
    
    # ===== CONCURRENCY TESTS (Step 3) =====
    log("\n" + "=" * 60)
    log("STEP 3: CONCURRENCY CONTROL TESTS")
    log("=" * 60)
    
    results["concurrency_tests"]["case1_concurrent_reads"] = run_test(
        "Case 1: Concurrent Reads",
        test_case1_concurrent_reads
    )
    
    results["concurrency_tests"]["case2_read_write_conflict"] = run_test(
        "Case 2: Read-Write Conflict",
        test_case2_read_write_conflict
    )
    
    results["concurrency_tests"]["case3_concurrent_writes"] = run_test(
        "Case 3: Concurrent Writes",
        test_case3_concurrent_writes
    )
    
    # ===== RECOVERY TESTS (Step 4) =====
    log("\n" + "=" * 60)
    log("STEP 4: GLOBAL FAILURE RECOVERY TESTS")
    log("=" * 60)
    
    results["recovery_tests"]["case1_fragment_to_central"] = run_test(
        "Case 1: Fragment → Central Failure Info",
        test_recovery_case1_fragment_to_central
    )
    
    results["recovery_tests"]["case2_central_recovery"] = run_test(
        "Case 2: Central Node Recovery",
        test_recovery_case2_central_recovery
    )
    
    results["recovery_tests"]["case3_central_to_fragment"] = run_test(
        "Case 3: Central → Fragment Failure Info",
        test_recovery_case3_central_to_fragment
    )
    
    results["recovery_tests"]["case4_fragment_recovery"] = run_test(
        "Case 4: Fragment Node Recovery",
        test_recovery_case4_fragment_recovery
    )
    
    # Recovery status check
    log("\nChecking recovery status...")
    status = check_recovery_status()
    results["recovery_tests"]["final_status"] = status
    log(f"  Pending replications: {status.get('total_pending', 'N/A')}")
    log(f"  Auto-retry active: {status.get('auto_retry_active', 'N/A')}")
    
    # ===== SUMMARY =====
    log("\n" + "=" * 60)
    log("TEST SUMMARY")
    log("=" * 60)
    
    total_tests = 0
    total_passed = 0
    
    for category in ["concurrency_tests", "recovery_tests"]:
        for test_name, test_data in results[category].items():
            if isinstance(test_data, dict) and "summary" in test_data:
                summary = test_data["summary"]
                total_tests += summary.get("total_runs", 0)
                total_passed += summary.get("successes", 0)
                log(f"  {test_name}: {summary.get('success_rate', 'N/A')}")
    
    overall_rate = f"{(total_passed/total_tests)*100:.0f}%" if total_tests > 0 else "N/A"
    results["summary"] = {
        "total_test_runs": total_tests,
        "total_passed": total_passed,
        "total_failed": total_tests - total_passed,
        "overall_success_rate": overall_rate
    }
    
    log("")
    log(f"OVERALL: {total_passed}/{total_tests} test runs passed ({overall_rate})")
    
    # Save results to file
    output_file = f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    log(f"\nResults saved to: {output_file}")
    log("=" * 60)
    
    return 0 if total_passed == total_tests else 1


if __name__ == "__main__":
    sys.exit(main())