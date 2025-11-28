import logging
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class ConcurrencyTester:
    def __init__(self, db_manager, replication_manager):
        self.db = db_manager
        self.replication_manager = replication_manager
    
    def test_concurrent_reads(self, tconst, isolation_level='READ COMMITTED'):
        """
        Test Case 1: Concurrent reads on same data from MULTIPLE NODES
        Tests if multiple transactions can read simultaneously without blocking
        """
        results = {}
        threads = []
        lock = threading.Lock()
        start_barrier = threading.Barrier(3)  # Ensure all start together
        
        # Find which nodes have this record
        title = self.db.get_title_by_id(tconst)
        if 'error' in title:
            return {'error': f'Title {tconst} not found'}
        
        title_type = title.get('title_type')
        
        # Determine test nodes
        if title_type == 'movie':
            test_nodes = ['node1', 'node2', 'node2']  # Test node2 twice for true concurrency
        else:
            test_nodes = ['node1', 'node3', 'node3']
        
        def concurrent_read(node_name, reader_id):
            """Each reader performs a transaction"""
            try:
                # Wait for all readers to be ready
                start_barrier.wait()
                
                start_time = time.time()
                conn = self.db.get_connection(node_name, isolation_level)
                
                if not conn:
                    with lock:
                        results[f'{node_name}_{reader_id}'] = {
                            'success': False,
                            'error': 'Connection failed'
                        }
                    return
                
                try:
                    conn.start_transaction()
                    cursor = conn.cursor(dictionary=True)
                    
                    # Perform multiple reads to simulate real workload
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    data1 = cursor.fetchone()
                    
                    # Second read in same transaction (tests REPEATABLE READ)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    data2 = cursor.fetchone()
                    
                    conn.commit()
                    end_time = time.time()
                    
                    with lock:
                        results[f'{node_name}_{reader_id}'] = {
                            'success': True,
                            'node': node_name,
                            'data': data1,
                            'repeatable': data1 == data2,
                            'duration': round(end_time - start_time, 4),
                            'isolation_level': isolation_level,
                            'timestamp': datetime.now().isoformat()
                        }
                        
                except Exception as e:
                    conn.rollback()
                    with lock:
                        results[f'{node_name}_{reader_id}'] = {
                            'success': False,
                            'error': str(e),
                            'node': node_name
                        }
                finally:
                    conn.close()
                    
            except Exception as e:
                with lock:
                    results[f'{node_name}_{reader_id}'] = {
                        'success': False,
                        'error': f'Barrier/setup error: {str(e)}'
                    }
        
        # Launch concurrent readers
        for i, node in enumerate(test_nodes):
            t = threading.Thread(target=concurrent_read, args=(node, i))
            threads.append(t)
            t.start()
        
        # Wait for completion
        for t in threads:
            t.join()
        
        # Analyze consistency
        successful_reads = [r for r in results.values() if r.get('success')]
        data_values = [str(r['data']) for r in successful_reads if r.get('data')]
        consistent = len(set(data_values)) <= 1
        
        return {
            'test': 'concurrent_reads',
            'test_case': 'Case #1',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'nodes_tested': test_nodes,
            'concurrent_readers': len(test_nodes),
            'results': results,
            'consistent': consistent,
            'all_reads_succeeded': len(successful_reads) == len(test_nodes),
            'analysis': {
                'blocking_observed': any(r.get('duration', 0) > 1 for r in successful_reads),
                'data_consistent': consistent,
                'explanation': self._explain_read_behavior(isolation_level, consistent)
            }
        }
    
    def test_read_write_conflict(self, tconst, new_data, isolation_level='READ COMMITTED'):
        """
        Test Case 2: One writer, multiple readers, SAME DATA, SAME TIME
        Tests isolation levels: dirty reads, non-repeatable reads, phantoms
        """
        results = {
            'original_value': {},
            'readers': {},
            'writer': {},
            'final_value': {}
        }
        lock = threading.Lock()
        
        # Record original
        original = self.db.get_title_by_id(tconst)
        if 'error' in original:
            return {'error': f'Title {tconst} not found'}
        
        results['original_value'] = original
        title_type = original.get('title_type')
        primary_node = 'node2' if title_type == 'movie' else 'node3'
        
        # Barrier ensures all start simultaneously
        participant_count = 4  # 1 writer + 3 readers
        start_barrier = threading.Barrier(participant_count)
        
        def writer_transaction():
            """Writer that holds transaction open"""
            try:
                start_barrier.wait()
                start_time = time.time()
                
                conn = self.db.get_connection(primary_node, isolation_level)
                if not conn:
                    with lock:
                        results['writer'] = {'success': False, 'error': 'Connection failed'}
                    return
                
                try:
                    conn.start_transaction()
                    cursor = conn.cursor()
                    
                    # Build UPDATE
                    set_clauses = []
                    params = []
                    for key, value in new_data.items():
                        if key != 'tconst':
                            set_clauses.append(f"{key} = %s")
                            params.append(value)
                    params.append(tconst)
                    
                    query = f"UPDATE titles SET {', '.join(set_clauses)} WHERE tconst = %s"
                    cursor.execute(query, tuple(params))
                    
                    # CRITICAL: Hold transaction open to test concurrent reads
                    time.sleep(0.5)  # Readers will try to read during this window
                    
                    conn.commit()
                    end_time = time.time()
                    
                    with lock:
                        results['writer'] = {
                            'success': True,
                            'node': primary_node,
                            'duration': round(end_time - start_time, 4),
                            'new_data': new_data
                        }
                        
                except Exception as e:
                    conn.rollback()
                    with lock:
                        results['writer'] = {'success': False, 'error': str(e)}
                finally:
                    conn.close()
                    
            except Exception as e:
                with lock:
                    results['writer'] = {'success': False, 'error': f'Barrier error: {str(e)}'}
        
        def reader_transaction(node_name, reader_id):
            """Reader that tries to read during write"""
            try:
                start_barrier.wait()
                
                # Small stagger so readers hit during the write
                time.sleep(0.05 * reader_id)
                
                start_time = time.time()
                conn = self.db.get_connection(node_name, isolation_level)
                
                if not conn:
                    with lock:
                        results['readers'][f'{node_name}_{reader_id}'] = {
                            'success': False,
                            'error': 'Connection failed'
                        }
                    return
                
                try:
                    conn.start_transaction()
                    cursor = conn.cursor(dictionary=True)
                    
                    # First read
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    read1 = cursor.fetchone()
                    
                    time.sleep(0.1)  # Wait a bit
                    
                    # Second read (tests REPEATABLE READ)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    read2 = cursor.fetchone()
                    
                    conn.commit()
                    end_time = time.time()
                    
                    # Check if read saw uncommitted data (dirty read)
                    saw_uncommitted = False
                    for key, new_val in new_data.items():
                        if key in read1 and read1[key] == new_val:
                            saw_uncommitted = True
                            break
                    
                    with lock:
                        results['readers'][f'{node_name}_{reader_id}'] = {
                            'success': True,
                            'node': node_name,
                            'read1': read1,
                            'read2': read2,
                            'repeatable': read1 == read2,
                            'dirty_read_detected': saw_uncommitted,
                            'duration': round(end_time - start_time, 4),
                            'blocked': end_time - start_time > 0.4
                        }
                        
                except Exception as e:
                    conn.rollback()
                    with lock:
                        results['readers'][f'{node_name}_{reader_id}'] = {
                            'success': False,
                            'error': str(e),
                            'node': node_name
                        }
                finally:
                    conn.close()
                    
            except Exception as e:
                with lock:
                    results['readers'][f'{node_name}_{reader_id}'] = {
                        'success': False,
                        'error': f'Barrier error: {str(e)}'
                    }
        
        # Launch 1 writer + 3 readers simultaneously
        threads = []
        
        # Writer thread
        writer_thread = threading.Thread(target=writer_transaction)
        threads.append(writer_thread)
        writer_thread.start()
        
        # Reader threads on different nodes
        reader_nodes = ['node1', primary_node, primary_node]
        for i, node in enumerate(reader_nodes):
            reader_thread = threading.Thread(target=reader_transaction, args=(node, i))
            threads.append(reader_thread)
            reader_thread.start()
        
        # Wait for all
        for t in threads:
            t.join()
        
        # Read final value
        time.sleep(0.2)
        results['final_value'] = self.db.get_title_by_id(tconst)
        
        # Analysis
        any_dirty_reads = any(
            r.get('dirty_read_detected', False) 
            for r in results['readers'].values() 
            if r.get('success')
        )
        
        any_blocking = any(
            r.get('blocked', False)
            for r in results['readers'].values()
            if r.get('success')
        )
        
        non_repeatable_reads = any(
            not r.get('repeatable', True)
            for r in results['readers'].values()
            if r.get('success')
        )
        
        return {
            'test': 'read_write_conflict',
            'test_case': 'Case #2',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'new_data': new_data,
            'results': results,
            'analysis': {
                'dirty_reads_occurred': any_dirty_reads,
                'blocking_occurred': any_blocking,
                'non_repeatable_reads': non_repeatable_reads,
                'explanation': self._explain_read_write_behavior(
                    isolation_level, 
                    any_dirty_reads, 
                    any_blocking, 
                    non_repeatable_reads
                )
            }
        }
    
    def test_concurrent_writes(self, updates, isolation_level='READ COMMITTED'):
        """
        Test Case 3: Multiple writers updating SAME RECORD simultaneously
        Tests deadlocks, lost updates, write conflicts
        
        CRITICAL: This tests DATABASE concurrency, not replication!
        All writes go to the SAME NODE to test locking behavior.
        """
        if not updates or len(updates) < 2:
            return {'error': 'Need at least 2 concurrent updates for Case #3'}
        
        # All updates should be to the SAME tconst for true conflict testing
        tconst = updates[0]['tconst']
        
        # Verify all updates are for same record
        if not all(u['tconst'] == tconst for u in updates):
            return {
                'error': 'Case #3 requires all updates to be on the SAME record',
                'hint': 'Use same tconst for all updates to test write conflicts'
            }
        
        title = self.db.get_title_by_id(tconst)
        if 'error' in title:
            return {'error': f'Title {tconst} not found'}
        
        title_type = title.get('title_type')
        target_node = 'node2' if title_type == 'movie' else 'node3'
        
        results = {
            'original_value': title,
            'writers': {},
            'final_value': {},
            'conflicts': []
        }
        lock = threading.Lock()
        
        # Barrier to ensure simultaneous start
        start_barrier = threading.Barrier(len(updates))
        
        def concurrent_writer(update_data, writer_id):
            """Each writer tries to update the SAME record"""
            try:
                start_barrier.wait()
                
                start_time = time.time()
                conn = self.db.get_connection(target_node, isolation_level)
                
                if not conn:
                    with lock:
                        results['writers'][f'writer_{writer_id}'] = {
                            'success': False,
                            'error': 'Connection failed'
                        }
                    return
                
                try:
                    conn.start_transaction()
                    cursor = conn.cursor()
                    
                    # Read current value (may cause lock)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s FOR UPDATE", (tconst,))
                    current = cursor.fetchone()
                    
                    # Build UPDATE
                    set_clauses = []
                    params = []
                    for key, value in update_data['data'].items():
                        if key != 'tconst':
                            set_clauses.append(f"{key} = %s")
                            params.append(value)
                    params.append(tconst)
                    
                    query = f"UPDATE titles SET {', '.join(set_clauses)} WHERE tconst = %s"
                    
                    # Simulate processing time
                    time.sleep(0.1)
                    
                    cursor.execute(query, tuple(params))
                    
                    conn.commit()
                    end_time = time.time()
                    
                    with lock:
                        results['writers'][f'writer_{writer_id}'] = {
                            'success': True,
                            'node': target_node,
                            'data_written': update_data['data'],
                            'duration': round(end_time - start_time, 4),
                            'waited_for_lock': end_time - start_time > 0.15,
                            'timestamp': datetime.now().isoformat()
                        }
                        
                except Exception as e:
                    conn.rollback()
                    error_msg = str(e)
                    
                    with lock:
                        results['writers'][f'writer_{writer_id}'] = {
                            'success': False,
                            'error': error_msg,
                            'deadlock': 'deadlock' in error_msg.lower(),
                            'lock_timeout': 'timeout' in error_msg.lower()
                        }
                        
                        if 'deadlock' in error_msg.lower():
                            results['conflicts'].append({
                                'type': 'deadlock',
                                'writer': writer_id,
                                'message': error_msg
                            })
                            
                finally:
                    conn.close()
                    
            except Exception as e:
                with lock:
                    results['writers'][f'writer_{writer_id}'] = {
                        'success': False,
                        'error': f'Setup error: {str(e)}'
                    }
        
        # Launch all writers simultaneously
        threads = []
        for i, update in enumerate(updates):
            t = threading.Thread(target=concurrent_writer, args=(update, i))
            threads.append(t)
            t.start()
        
        # Wait for all
        for t in threads:
            t.join()
        
        # Read final value
        time.sleep(0.2)
        results['final_value'] = self.db.get_title_by_id(tconst)
        
        # Analysis
        successful_writers = [
            w for w in results['writers'].values() 
            if w.get('success')
        ]
        
        failed_writers = [
            w for w in results['writers'].values()
            if not w.get('success')
        ]
        
        deadlocks = len(results['conflicts'])
        
        blocking_occurred = any(
            w.get('waited_for_lock', False)
            for w in successful_writers
        )
        
        return {
            'test': 'concurrent_writes',
            'test_case': 'Case #3',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'target_node': target_node,
            'concurrent_writers': len(updates),
            'results': results,
            'analysis': {
                'successful_writes': len(successful_writers),
                'failed_writes': len(failed_writers),
                'deadlocks_detected': deadlocks,
                'blocking_occurred': blocking_occurred,
                'serialization_enforced': blocking_occurred or deadlocks > 0,
                'explanation': self._explain_write_behavior(
                    isolation_level,
                    len(successful_writers),
                    deadlocks,
                    blocking_occurred
                )
            }
        }
    
    def _explain_read_behavior(self, isolation_level, consistent):
        if isolation_level == 'READ UNCOMMITTED':
            return 'READ UNCOMMITTED: Allows dirty reads, lowest isolation, highest concurrency'
        elif isolation_level == 'READ COMMITTED':
            return 'READ COMMITTED: No dirty reads, but non-repeatable reads possible'
        elif isolation_level == 'REPEATABLE READ':
            return 'REPEATABLE READ: No dirty or non-repeatable reads, but phantom reads possible'
        else:
            return 'SERIALIZABLE: Full isolation, transactions appear to execute sequentially'
    
    def _explain_read_write_behavior(self, isolation_level, dirty, blocking, non_repeatable):
        explanations = []
        
        if isolation_level == 'READ UNCOMMITTED':
            explanations.append('READ UNCOMMITTED allows dirty reads')
            if dirty:
                explanations.append('✗ Dirty read detected: Reader saw uncommitted changes')
        elif isolation_level == 'READ COMMITTED':
            explanations.append('READ COMMITTED prevents dirty reads')
            if non_repeatable:
                explanations.append('⚠ Non-repeatable read: Same query returned different results')
        elif isolation_level == 'REPEATABLE READ':
            explanations.append('REPEATABLE READ ensures consistent reads within transaction')
            if blocking:
                explanations.append('✓ Blocking observed: Writer held lock until commit')
        else:
            explanations.append('SERIALIZABLE: Maximum isolation, may cause significant blocking')
        
        return ' | '.join(explanations)
    
    def _explain_write_behavior(self, isolation_level, successful, deadlocks, blocking):
        explanations = []
        
        if deadlocks > 0:
            explanations.append(f'✗ {deadlocks} deadlock(s) detected')
        
        if blocking:
            explanations.append('✓ Locking enforced: Writers waited for each other')
        
        if isolation_level == 'SERIALIZABLE':
            explanations.append('SERIALIZABLE: Strictest conflict detection')
        elif isolation_level == 'REPEATABLE READ':
            explanations.append('REPEATABLE READ: Row-level locking prevents lost updates')
        else:
            explanations.append(f'{isolation_level}: May allow some concurrent writes')
        
        explanations.append(f'{successful} writer(s) succeeded')
        
        return ' | '.join(explanations)
    
    def simulate_failure(self, scenario):
        """Guide for simulating failure scenarios"""
        if scenario == 'fragment_to_central':
            return {
                'scenario': 'Case #1: Central node failure during replication',
                'description': 'Fragment write succeeds, but central replication fails',
                'steps': [
                    '1. Stop node1 container: docker stop node1-central',
                    '2. Insert a new title via POST /title',
                    '3. Check pending queue: GET /recovery/status',
                    '4. Restart node1: docker start node1-central',
                    '5. Trigger recovery: POST /test/failure/central-recovery'
                ],
                'expected': 'Insert succeeds on fragment, queued for central',
                'current_pending': self.replication_manager.recovery_handler.get_pending_count()
            }
        
        elif scenario == 'central_to_fragment':
            return {
                'scenario': 'Case #3: Fragment node failure during replication',
                'description': 'Central write succeeds (fallback), but fragment replication fails',
                'steps': [
                    '1. Stop node2 container: docker stop node2-movies',
                    '2. Insert a movie via POST /title (will use central as fallback)',
                    '3. Check pending queue: GET /recovery/status',
                    '4. Restart node2: docker start node2-movies',
                    '5. Trigger recovery: POST /test/failure/fragment-recovery with {"node": "node2"}'
                ],
                'expected': 'Insert succeeds on central, queued for fragment',
                'current_pending': self.replication_manager.recovery_handler.get_pending_count()
            }
        
        else:
            return {
                'error': 'Unknown scenario',
                'valid_scenarios': [
                    'fragment_to_central',
                    'central_to_fragment'
                ]
            }