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
        Test Case 1: Concurrent transactions reading the same data item
        
        Per specs: "Concurrent transactions in two or more nodes are reading the same data item"
        
        Setup:
        - 3 concurrent readers
        - Each reads from nodes that have the record
        - Tests if reads block each other
        - Tests if reads are consistent
        """
        results = {}
        threads = []
        lock = threading.Lock()
        start_barrier = threading.Barrier(3)  # 3 concurrent readers
        
        # Find which nodes have this record
        title = self.db.get_title_by_id(tconst)
        if 'error' in title:
            return {'error': f'Title {tconst} not found'}
        
        title_type = title.get('title_type')
        
        # Test reads from multiple nodes that have the data
        # Node1 always has it (central), plus appropriate fragment
        if title_type == 'movie':
            test_nodes = ['node1', 'node2', 'node2']  # Central + fragment twice
        else:
            test_nodes = ['node1', 'node3', 'node3']  # Central + fragment twice
        
        def concurrent_read(node_name, reader_id):
            """Each reader performs a transaction with 2 reads"""
            try:
                # Wait for all readers to be ready (synchronized start)
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
                    
                    # First read
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    data1 = cursor.fetchone()
                    read1_time = time.time()
                    
                    time.sleep(0.1)  # Simulate processing
                    
                    # Second read in same transaction (tests REPEATABLE READ)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    data2 = cursor.fetchone()
                    read2_time = time.time()
                    
                    conn.commit()
                    end_time = time.time()
                    
                    with lock:
                        results[f'{node_name}_{reader_id}'] = {
                            'success': True,
                            'node': node_name,
                            'reader_id': reader_id,
                            'data': data1,
                            'repeatable': data1 == data2,
                            'duration': round(end_time - start_time, 4),
                            'read1_time': round(read1_time - start_time, 4),
                            'read2_time': round(read2_time - start_time, 4),
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
            'description': 'Concurrent transactions in two or more nodes reading the same data item',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'nodes_tested': test_nodes,
            'concurrent_readers': len(test_nodes),
            'results': results,
            'consistent': consistent,
            'all_reads_succeeded': len(successful_reads) == len(test_nodes),
            'analysis': {
                'blocking_observed': any(r.get('duration', 0) > 1 for r in successful_reads),
                'data_consistent_across_nodes': consistent,
                'repeatable_reads_working': all(r.get('repeatable', False) for r in successful_reads),
                'explanation': self._explain_read_behavior(isolation_level, consistent)
            }
        }
    
    def test_read_write_conflict(self, tconst, new_data, isolation_level='READ COMMITTED'):
        """
        Test Case 2: At least one transaction writing while others read the same data item
        
        Per specs: "At least one transaction in the three nodes is writing (update/deletion) 
        and the others are reading the same data item"
        
        Setup:
        - 2 writers (node1 + fragment) updating the SAME record
        - 2 readers (node1 + fragment) reading during the write
        - Tests dirty reads, non-repeatable reads, blocking behavior
        """
        results = {
            'original_value': {},
            'writers': {},
            'readers': {},
            'final_values': {},
            'note': 'Tests MySQL isolation across distributed nodes (database-level concurrency)'
        }
        lock = threading.Lock()
        
        # Record original
        original = self.db.get_title_by_id(tconst)
        if 'error' in original:
            return {'error': f'Title {tconst} not found'}
        
        results['original_value'] = original
        title_type = original.get('title_type')
        fragment_node = 'node2' if title_type == 'movie' else 'node3'
        
        # Build UPDATE query parts BEFORE creating threads
        set_clauses = []
        params = []
        for key, value in new_data.items():
            if key != 'tconst':
                set_clauses.append(f"{key} = %s")
                params.append(value)
        params.append(tconst)
        
        # Setup: 2 writers + 2 readers = 4 participants
        participant_count = 4
        start_barrier = threading.Barrier(participant_count)
        
        def writer_transaction(node_name, writer_id):
            """Writer that holds transaction open to test concurrent reads"""
            try:
                start_barrier.wait()
                start_time = time.time()
                
                conn = self.db.get_connection(node_name, isolation_level)
                if not conn:
                    with lock:
                        results['writers'][f'{node_name}_writer_{writer_id}'] = {
                            'success': False, 'error': 'Connection failed'
                        }
                    return
                
                try:
                    conn.start_transaction()
                    cursor = conn.cursor()
                    
                    # Execute UPDATE
                    query = f"UPDATE titles SET {', '.join(set_clauses)} WHERE tconst = %s"
                    cursor.execute(query, tuple(params))
                    
                    logger.info(f"[Case #2] Writer {writer_id} on {node_name} executed UPDATE")
                    
                    # HOLD transaction to ensure readers can encounter it
                    time.sleep(0.5) 
                    
                    conn.commit()
                    commit_time = time.time()
                    
                    logger.info(f"[Case #2] Writer {writer_id} COMMITTED after {commit_time - start_time:.4f}s")
                    
                    with lock:
                        results['writers'][f'{node_name}_writer_{writer_id}'] = {
                            'success': True,
                            'node': node_name,
                            'writer_id': writer_id,
                            'commit_time': round(commit_time - start_time, 4),
                            'rows_affected': cursor.rowcount,
                            'duration': round(commit_time - start_time, 4),
                            'timestamp': datetime.now().isoformat()
                        }
                        
                except Exception as e:
                    conn.rollback()
                    logger.error(f"[Case #2] Writer {writer_id} error: {e}")
                    with lock:
                        results['writers'][f'{node_name}_writer_{writer_id}'] = {
                            'success': False,
                            'error': str(e),
                            'node': node_name,
                            'deadlock': 'deadlock' in str(e).lower()
                        }
                finally:
                    conn.close()
                    
            except Exception as e:
                with lock:
                    results['writers'][f'{node_name}_writer_{writer_id}'] = {
                        'success': False,
                        'error': f'Barrier error: {str(e)}'
                    }
        
        def reader_transaction(node_name, reader_id):
            """Reader that attempts to read during writer's transaction"""
            try:
                start_barrier.wait()
                time.sleep(0.02 * (reader_id + 1))  # Slight stagger to ensure overlap with writers
                
                read_start_time = time.time()  # FIX: Define this variable
                conn = self.db.get_connection(node_name, isolation_level)
                
                if not conn:
                    with lock:
                        results['readers'][f'{node_name}_reader_{reader_id}'] = {
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
                    read1_time = time.time()
                    
                    time.sleep(0.1)
                    
                    # Second read (tests REPEATABLE READ within same transaction)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    read2 = cursor.fetchone()
                    read2_time = time.time()
                    
                    conn.commit()
                    end_time = time.time()
                    
                    # Detect if reader saw the new value
                    read_during_write = (read1_time - read_start_time) < 0.4
            
                    saw_new_value = False
                    if read1 is not None:
                        for key, new_val in new_data.items():
                            if key in read1 and read1[key] == new_val:
                                saw_new_value = True
                                break
                    
                    # Dirty read = saw new value WHILE writer was still uncommitted
                    is_dirty_read = saw_new_value and read_during_write
                    
                    # Blocking = transaction took long (reader waited for writer)
                    was_blocked = (end_time - read_start_time) > 0.3
                    
                    with lock:
                        results['readers'][f'{node_name}_reader_{reader_id}'] = {
                            'success': True,
                            'node': node_name,
                            'reader_id': reader_id,
                            'read1': read1,
                            'read2': read2,
                            'read1_timestamp': round(read1_time - read_start_time, 4),
                            'read2_timestamp': round(read2_time - read_start_time, 4),
                            'read_during_write': read_during_write,
                            'repeatable': read1 == read2,
                            'saw_new_value': saw_new_value,
                            'dirty_read_detected': is_dirty_read,
                            'blocked': was_blocked,
                            'duration': round(end_time - read_start_time, 4),
                            'timestamp': datetime.now().isoformat()
                        }
                        
                except Exception as e:
                    conn.rollback()
                    logger.error(f"[Case #2] Reader {reader_id} error: {e}")
                    with lock:
                        results['readers'][f'{node_name}_reader_{reader_id}'] = {
                            'success': False,
                            'error': str(e),
                            'node': node_name
                        }
                finally:
                    conn.close()
                    
            except Exception as e:
                with lock:
                    results['readers'][f'{node_name}_reader_{reader_id}'] = {
                        'success': False,
                        'error': f'Barrier error: {str(e)}'
                    }
        
        # Launch threads: 2 writers + 2 readers
        threads = []
        
        # Writer 1: Central node
        writer1 = threading.Thread(target=writer_transaction, args=('node1', 0))
        threads.append(writer1)
        writer1.start()
        
        # Writer 2: Fragment node
        writer2 = threading.Thread(target=writer_transaction, args=(fragment_node, 1))
        threads.append(writer2)
        writer2.start()
        
        # Reader 1: Read from central
        reader1 = threading.Thread(target=reader_transaction, args=('node1', 0))
        threads.append(reader1)
        reader1.start()
        
        # Reader 2: Read from fragment
        reader2 = threading.Thread(target=reader_transaction, args=(fragment_node, 1))
        threads.append(reader2)
        reader2.start()
        
        # Wait for all threads
        for t in threads:
            t.join()
        
        # Read final values from BOTH nodes
        time.sleep(0.3)
        
        # Final value on node1
        conn1 = self.db.get_connection('node1')
        if conn1:
            try:
                cursor = conn1.cursor(dictionary=True)
                cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                results['final_values']['node1'] = cursor.fetchone()
            except Exception as e:
                results['final_values']['node1'] = {'error': str(e)}
            finally:
                conn1.close()
        
        # Final value on fragment node
        conn_frag = self.db.get_connection(fragment_node)
        if conn_frag:
            try:
                cursor = conn_frag.cursor(dictionary=True)
                cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                results['final_values'][fragment_node] = cursor.fetchone()
            except Exception as e:
                results['final_values'][fragment_node] = {'error': str(e)}
            finally:
                conn_frag.close()
        
        # Analysis
        successful_writers = [w for w in results['writers'].values() if w.get('success')]
        successful_readers = [r for r in results['readers'].values() if r.get('success')]
        
        any_dirty_reads = any(r.get('dirty_read_detected', False) for r in successful_readers)
        any_blocking = any(r.get('blocked', False) for r in successful_readers)
        non_repeatable_reads = any(not r.get('repeatable', True) for r in successful_readers)
        
        # Check if final values are consistent across nodes
        # Note: This checks database-level consistency, NOT app-level replication
        final_vals = [str(v) for v in results['final_values'].values() 
                      if v and (not isinstance(v, dict) or 'error' not in v)]
        nodes_consistent = len(set(final_vals)) <= 1 if final_vals else False
        
        return {
            'test': 'read_write_conflict',
            'test_case': 'Case #2',
            'description': 'At least one transaction writing while others read the same data item',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'new_data': new_data,
            'nodes_involved': ['node1', fragment_node],
            'note': 'Tests MySQL isolation behavior across distributed nodes',
            'results': results,
            'analysis': {
                'writers_succeeded': len(successful_writers),
                'readers_succeeded': len(successful_readers),
                'dirty_reads_occurred': any_dirty_reads,
                'blocking_occurred': any_blocking,
                'non_repeatable_reads': non_repeatable_reads,
                'final_state_consistent_across_nodes': nodes_consistent,
                'explanation': self._explain_read_write_behavior(
                    isolation_level, any_dirty_reads, any_blocking, non_repeatable_reads
                )
            }
        }
    
    def test_concurrent_writes(self, tconst, updates, isolation_level='READ COMMITTED'):
        """
        Test Case 3: Concurrent transactions writing (update/deletion) on the same data item
        
        Per specs: "Concurrent transactions in two or more nodes are writing (update/deletion) 
        on the same data item"
        
        Setup:
        - 3 writers all updating the SAME record (same tconst)
        - Writer 1 on node1 (central)
        - Writer 2 on fragment
        - Writer 3 on fragment
        - Tests deadlocks, lost updates, write conflicts
        """
        if not updates or len(updates) < 2:
            return {'error': 'Need at least 2 concurrent updates for Case #3'}
        
        # All updates MUST target the SAME tconst
        if not all(u.get('tconst') == tconst for u in updates):
            return {
                'error': 'Case #3 requires all updates to target the SAME record (same tconst)',
                'hint': 'All updates array items must have the same tconst value'
            }
        
        title = self.db.get_title_by_id(tconst)
        if 'error' in title:
            return {'error': f'Title {tconst} not found'}
        
        title_type = title.get('title_type')
        fragment_node = 'node2' if title_type == 'movie' else 'node3'
        
        results = {
            'original_value': title,
            'writers': {},
            'final_values': {},
            'conflicts': []
        }
        lock = threading.Lock()
        
        # Writers target DIFFERENT nodes but SAME record
        writer_configs = [
            {'node': 'node1', 'data': updates[0]},
            {'node': fragment_node, 'data': updates[1]},
            {'node': fragment_node, 'data': updates[2] if len(updates) > 2 else updates[1]}
        ]
        
        # Barrier to ensure simultaneous start
        start_barrier = threading.Barrier(len(writer_configs))
        
        def concurrent_writer(node_name, update_data, writer_id):
            """Each writer tries to update the SAME record on its node"""
            try:
                start_barrier.wait()
                
                start_time = time.time()
                conn = self.db.get_connection(node_name, isolation_level)
                
                if not conn:
                    with lock:
                        results['writers'][f'writer_{writer_id}_{node_name}'] = {
                            'success': False,
                            'error': 'Connection failed',
                            'node': node_name
                        }
                    return
                
                try:
                    conn.start_transaction()
                    cursor = conn.cursor()
                    
                    # Read current value with FOR UPDATE lock
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s FOR UPDATE", (tconst,))
                    current = cursor.fetchone()
                    
                    # Build UPDATE
                    set_clauses = []
                    params = []
                    for key, value in update_data.get('data', {}).items():
                        if key != 'tconst':
                            set_clauses.append(f"{key} = %s")
                            params.append(value)
                    params.append(tconst)
                    
                    query = f"UPDATE titles SET {', '.join(set_clauses)} WHERE tconst = %s"
                    
                    # Simulate processing time to increase chance of conflicts
                    time.sleep(0.15)
                    
                    logger.info(f"[Case #3] Writer {writer_id} executing UPDATE on {node_name}")
                    cursor.execute(query, tuple(params))
                    
                    conn.commit()
                    end_time = time.time()
                    
                    logger.info(f"[Case #3] Writer {writer_id} committed on {node_name}")
                    
                    with lock:
                        results['writers'][f'writer_{writer_id}_{node_name}'] = {
                            'success': True,
                            'node': node_name,
                            'writer_id': writer_id,
                            'data_written': update_data.get('data'),
                            'duration': round(end_time - start_time, 4),
                            'waited_for_lock': end_time - start_time > 0.2,
                            'timestamp': datetime.now().isoformat()
                        }
                        
                except Exception as e:
                    conn.rollback()
                    error_msg = str(e)
                    
                    logger.error(f"[Case #3] Writer {writer_id} on {node_name} failed: {error_msg}")
                    
                    with lock:
                        results['writers'][f'writer_{writer_id}_{node_name}'] = {
                            'success': False,
                            'error': error_msg,
                            'node': node_name,
                            'deadlock': 'deadlock' in error_msg.lower(),
                            'lock_timeout': 'timeout' in error_msg.lower()
                        }
                        
                        if 'deadlock' in error_msg.lower():
                            results['conflicts'].append({
                                'type': 'deadlock',
                                'writer': writer_id,
                                'node': node_name,
                                'message': error_msg
                            })
                            
                finally:
                    conn.close()
                    
            except Exception as e:
                with lock:
                    results['writers'][f'writer_{writer_id}_{node_name}'] = {
                        'success': False,
                        'error': f'Setup error: {str(e)}',
                        'node': node_name
                    }
        
        # Launch all writers simultaneously
        threads = []
        for i, config in enumerate(writer_configs):
            t = threading.Thread(
                target=concurrent_writer,
                args=(config['node'], config['data'], i)
            )
            threads.append(t)
            t.start()
        
        # Wait for all
        for t in threads:
            t.join()
        
        # Read final values from ALL nodes
        time.sleep(0.3)
        
        # Final value on node1
        conn1 = self.db.get_connection('node1')
        if conn1:
            try:
                cursor = conn1.cursor(dictionary=True)
                cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                results['final_values']['node1'] = cursor.fetchone()
            except Exception as e:
                results['final_values']['node1'] = {'error': str(e)}
            finally:
                conn1.close()
        
        # Final value on fragment
        conn_frag = self.db.get_connection(fragment_node)
        if conn_frag:
            try:
                cursor = conn_frag.cursor(dictionary=True)
                cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                results['final_values'][fragment_node] = cursor.fetchone()
            except Exception as e:
                results['final_values'][fragment_node] = {'error': str(e)}
            finally:
                conn_frag.close()
        
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
        
        # Check consistency across nodes (database-level, not app replication)
        final_vals = [str(v) for v in results['final_values'].values() 
                      if v and (not isinstance(v, dict) or 'error' not in v)]
        nodes_consistent = len(set(final_vals)) <= 1 if final_vals else False
        
        return {
            'test': 'concurrent_writes',
            'test_case': 'Case #3',
            'description': 'Concurrent transactions writing (update/deletion) on the same data item',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'nodes_involved': ['node1', fragment_node],
            'concurrent_writers': len(writer_configs),
            'note': 'Tests how MySQL handles concurrent writes on the same record across nodes',
            'results': results,
            'analysis': {
                'successful_writes': len(successful_writers),
                'failed_writes': len(failed_writers),
                'deadlocks_detected': deadlocks,
                'blocking_occurred': blocking_occurred,
                'serialization_enforced': blocking_occurred or deadlocks > 0,
                'final_state_consistent_across_nodes': nodes_consistent,
                'explanation': self._explain_write_behavior(
                    isolation_level,
                    len(successful_writers),
                    deadlocks,
                    blocking_occurred
                )
            }
        }
    
    def _explain_read_behavior(self, isolation_level, consistent):
        """Explain what's expected for concurrent reads"""
        if isolation_level == 'READ UNCOMMITTED':
            return 'READ UNCOMMITTED: Allows dirty reads, lowest isolation, highest concurrency. All readers should succeed without blocking.'
        elif isolation_level == 'READ COMMITTED':
            return 'READ COMMITTED: No dirty reads, but non-repeatable reads possible. Readers should not block each other.'
        elif isolation_level == 'REPEATABLE READ':
            return 'REPEATABLE READ: No dirty or non-repeatable reads. Multiple reads in same transaction return identical results.'
        else:
            return 'SERIALIZABLE: Full isolation, transactions appear sequential. May cause more blocking.'
    
    def _explain_read_write_behavior(self, isolation_level, dirty, blocking, non_repeatable):
        """Explain read/write interaction behavior"""
        explanations = []
        
        if isolation_level == 'READ UNCOMMITTED':
            explanations.append('READ UNCOMMITTED: Allows dirty reads (reading uncommitted data)')
            if dirty:
                explanations.append('✗ Dirty read detected: Reader saw uncommitted changes')
            else:
                explanations.append('✓ No dirty reads observed')
                
        elif isolation_level == 'READ COMMITTED':
            explanations.append('READ COMMITTED: Prevents dirty reads, allows non-repeatable reads')
            if dirty:
                explanations.append('✗ BUG: Dirty read should not occur in READ COMMITTED!')
            if non_repeatable:
                explanations.append('⚠ Non-repeatable read: Same query returned different results')
            else:
                explanations.append('✓ Reads were repeatable within transaction')
                
        elif isolation_level == 'REPEATABLE READ':
            explanations.append('REPEATABLE READ: Ensures consistent reads within transaction')
            if blocking:
                explanations.append('✓ Blocking observed: Readers waited for writers')
            if non_repeatable:
                explanations.append('✗ BUG: Reads should be repeatable in this level!')
                
        else:  # SERIALIZABLE
            explanations.append('SERIALIZABLE: Maximum isolation, full serializability')
            if blocking:
                explanations.append('✓ Expected blocking behavior observed')
            if dirty:
                explanations.append('✗ CRITICAL BUG: Dirty reads impossible in SERIALIZABLE!')
        
        return ' | '.join(explanations)
    
    def _explain_write_behavior(self, isolation_level, successful, deadlocks, blocking):
        """Explain concurrent write behavior"""
        explanations = []
        
        if deadlocks > 0:
            explanations.append(f'⚠ {deadlocks} deadlock(s) detected - MySQL rolled back conflicting transaction')
        
        if blocking:
            explanations.append('✓ Locking enforced: Writers waited for each other (prevents lost updates)')
        
        if isolation_level == 'SERIALIZABLE':
            explanations.append('SERIALIZABLE: Strictest conflict detection, maximum blocking')
        elif isolation_level == 'REPEATABLE READ':
            explanations.append('REPEATABLE READ: Row-level locking prevents lost updates')
        else:
            explanations.append(f'{isolation_level}: May allow some concurrent writes with less blocking')
        
        explanations.append(f'{successful} writer(s) succeeded')
        
        return ' | '.join(explanations)
    
    def simulate_failure(self, scenario):
        """Guide for simulating failure scenarios (for Step 4 - Recovery)"""
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