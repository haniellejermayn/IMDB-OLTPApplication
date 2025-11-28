import logging
import threading
import time
import random
from datetime import datetime

logger = logging.getLogger(__name__)

class ConcurrencyTester:
    def __init__(self, db_manager, replication_manager):
        self.db = db_manager
        self.replication_manager = replication_manager
    
    def _get_test_record(self):
        """
        Get a record suitable for testing.
        Prefers records that exist in both central and fragment nodes.
        """
        # Try to get a movie (exists in node1 and node2)
        conn = self.db.get_connection('node1')
        if conn:
            try:
                cursor = conn.cursor(dictionary=True)
                cursor.execute("""
                    SELECT tconst, title_type, runtime_minutes 
                    FROM titles 
                    WHERE title_type = 'movie' 
                    AND runtime_minutes IS NOT NULL
                    LIMIT 1
                """)
                record = cursor.fetchone()
                if record:
                    return record['tconst']
            except Exception as e:
                logger.warning(f"Error getting test record: {e}")
            finally:
                conn.close()
        
        # Fallback: use a known test record
        return 'tt0035423'
    
    def test_concurrent_reads(self, tconst=None, isolation_level='READ COMMITTED'):
        """
        Test Case 1: Concurrent transactions reading the same data item
        
        Per specs: "Concurrent transactions in two or more nodes are reading the same data item"
        
        Setup:
        - 3 concurrent readers
        - Each reads from nodes that have the record
        - Tests if reads block each other
        - Tests if reads are consistent
        """
        # Auto-generate tconst if not provided
        if not tconst:
            tconst = self._get_test_record()
            logger.info(f"[Case #1] Auto-selected test record: {tconst}")
        
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
                'average_duration': round(sum(r.get('duration', 0) for r in successful_reads) / len(successful_reads), 4) if successful_reads else 0,
                'explanation': self._explain_read_behavior(isolation_level, consistent)
            }
        }
    
    def test_read_write_conflict(self, tconst=None, new_data=None, isolation_level='READ COMMITTED'):
        """
        Test Case 2: At least one transaction writing while others read the same data item
        
        Per specs: "At least one transaction in the three nodes is writing (update/deletion) 
        and the others are reading the same data item"
        
        Setup:
        - 2 writers (node1 + fragment) updating the SAME record
        - 2 readers (node1 + fragment) reading during the write
        - Tests dirty reads, non-repeatable reads, blocking behavior
        """
        # Auto-generate tconst if not provided
        if not tconst:
            tconst = self._get_test_record()
            logger.info(f"[Case #2] Auto-selected test record: {tconst}")
        
        # USE PROVIDED DATA OR GENERATE RANDOM
        if not new_data:
            unique_runtime = random.randint(1, 300)
            new_data = {'runtime_minutes': unique_runtime}
        else:
            # Extract just the value for logging/metadata if needed
            unique_runtime = new_data.get('runtime_minutes', random.randint(1, 300))

        results = {
            'original_value': {},
            'writers': {},
            'readers': {},
            'final_values': {},
            'test_metadata': {
                'unique_runtime_for_this_test': unique_runtime,
                'data_used': new_data,
                'note': 'Tests MySQL isolation across distributed nodes (database-level concurrency)'
            }
        }
        lock = threading.Lock()
        
        # Record original
        original = self.db.get_title_by_id(tconst)
        if 'error' in original:
            return {'error': f'Title {tconst} not found'}
        
        results['original_value'] = original
        original_runtime = original.get('runtime_minutes')
        title_type = original.get('title_type')
        fragment_node = 'node2' if title_type == 'movie' else 'node3'
        
        # Build UPDATE query parts based on new_data
        set_clauses = []
        params = []
        for k, v in new_data.items():
            set_clauses.append(f"{k} = %s")
            params.append(v)
        
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
                
                read_start_time = time.time()
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
                    
                    # Analysis
                    read_during_write = (read1_time - read_start_time) < 0.4
                    
                    # Helper to get specific field or full dict
                    current_runtime = read1.get('runtime_minutes') if read1 else None
                    
                    # Logic: Did we see the SPECIFIC new value being written?
                    # Note: This simple logic assumes we are updating runtime_minutes. 
                    # If updating other fields, this specific "dirty read" check might need adjustment.
                    saw_uncommitted_write = False
                    if 'runtime_minutes' in new_data:
                         saw_uncommitted_write = (current_runtime == new_data['runtime_minutes'] and 
                                                  current_runtime != original_runtime)
                    
                    is_dirty_read = saw_uncommitted_write and read_during_write
                    
                    # Non-repeatable read = read1 != read2 within same transaction
                    non_repeatable = read1 != read2
                    
                    # Blocking = transaction took long (reader waited for writer)
                    was_blocked = (end_time - read_start_time) > 0.3
                    
                    with lock:
                        results['readers'][f'{node_name}_reader_{reader_id}'] = {
                            'success': True,
                            'node': node_name,
                            'reader_id': reader_id,
                            'read1': read1,
                            'read2': read2,
                            'original_runtime': original_runtime,
                            'read_runtime': current_runtime,
                            'read1_timestamp': round(read1_time - read_start_time, 4),
                            'read2_timestamp': round(read2_time - read_start_time, 4),
                            'read_during_write': read_during_write,
                            'repeatable': not non_repeatable,
                            'saw_uncommitted_write': saw_uncommitted_write,
                            'dirty_read_detected': is_dirty_read,
                            'non_repeatable_read': non_repeatable,
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
        non_repeatable_reads = any(r.get('non_repeatable_read', False) for r in successful_readers)
        
        # Check if final values are consistent across nodes
        final_vals = []
        for node, val in results['final_values'].items():
            if val and not isinstance(val, dict):
                final_vals.append(str(val))
            elif val and 'error' not in val:
                final_vals.append(str(val))
        
        nodes_consistent = len(set(final_vals)) <= 1 if final_vals else False
        
        avg_reader_duration = round(sum(r.get('duration', 0) for r in successful_readers) / len(successful_readers), 4) if successful_readers else 0
        avg_writer_duration = round(sum(w.get('duration', 0) for w in successful_writers) / len(successful_writers), 4) if successful_writers else 0
        
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
                'average_reader_duration': avg_reader_duration,
                'average_writer_duration': avg_writer_duration,
                'explanation': self._explain_read_write_behavior(
                    isolation_level, any_dirty_reads, any_blocking, non_repeatable_reads
                )
            }
        }
    
    def test_concurrent_writes(self, updates=None, isolation_level='READ COMMITTED'):
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
        # Auto-generate tconst if not provided
        tconst = None
        writer_configs = []

        # --- BRANCH A: USER PROVIDED UPDATES ---
        if updates and isinstance(updates, list):
            # Extract tconst from the first update object
            tconst = updates[0].get('tconst')
            logger.info(f"[Case #3] Using provided updates for tconst: {tconst}")

            # Resolve fragment node based on movie type
            title = self.db.get_title_by_id(tconst)
            if 'error' in title:
                return {'error': f'Title {tconst} not found'}
            
            title_type = title.get('title_type')
            fragment_node = 'node2' if title_type == 'movie' else 'node3'

            # Map updates to nodes
            # Strategy: First update goes to Central, subsequent updates go to Fragment
            # This ensures distributed conflict
            for i, update_item in enumerate(updates):
                target_node = 'node1' if i == 0 else fragment_node
                writer_configs.append({
                    'node': target_node,
                    'data': update_item # Contains {'tconst': '...', 'data': {...}}
                })

        # --- BRANCH B: AUTO-GENERATE (Fallback) ---
        else:
            tconst = self._get_test_record()
            logger.info(f"[Case #3] Auto-selected test record: {tconst}")
            
            title = self.db.get_title_by_id(tconst)
            if 'error' in title:
                return {'error': f'Title {tconst} not found'}

            title_type = title.get('title_type')
            fragment_node = 'node2' if title_type == 'movie' else 'node3'

            # Generate random updates
            runtime1 = random.randint(1, 100)
            runtime2 = random.randint(101, 200)
            runtime3 = random.randint(201, 300)

            updates_generated = [
                {'tconst': tconst, 'data': {'runtime_minutes': runtime1}},
                {'tconst': tconst, 'data': {'runtime_minutes': runtime2}},
                {'tconst': tconst, 'data': {'runtime_minutes': runtime3}}
            ]

            writer_configs = [
                {'node': 'node1', 'data': updates_generated[0]},
                {'node': fragment_node, 'data': updates_generated[1]},
                {'node': fragment_node, 'data': updates_generated[2]}
            ]
        
        # --- COMMON EXECUTION LOGIC ---
        results = {
            'writers': {},
            'final_values': {},
            'conflicts': [],
            'test_metadata': {
                'tconst': tconst,
                'isolation_level': isolation_level,
                'note': 'Concurrent writes on same record'
            }
        }
        lock = threading.Lock()

        # Barrier to ensure simultaneous start
        start_barrier = threading.Barrier(len(writer_configs))
        
        def concurrent_writer(node_name, update_payload, writer_id):
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
                    
                    # Read current value with FOR UPDATE lock (simulating careful update)
                    # Note: In READ UNCOMMITTED/COMMITTED, this might behave differently regarding locks
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s FOR UPDATE", (tconst,))
                    # MUST READ THE RESULT to clear the cursor buffer!
                    _ = cursor.fetchall()
                    
                    # Build UPDATE
                    set_clauses = []
                    params = []
                    # update_payload['data'] contains the actual fields (runtime_minutes, etc)
                    data_dict = update_payload.get('data', {})
                    
                    for key, value in data_dict.items():
                        if key != 'tconst':
                            set_clauses.append(f"{key} = %s")
                            params.append(value)
                    params.append(tconst)
                    
                    if not set_clauses:
                        raise Exception("No data fields to update provided")

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
                            'data_written': data_dict,
                            'duration': round(end_time - start_time, 4),
                            'waited_for_lock': end_time - start_time > 0.2,
                            'rows_affected': cursor.rowcount,
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
        # We need to re-resolve fragment node just in case tconst changed (though unlikely)
        fragment_node_final = fragment_node 
        conn_frag = self.db.get_connection(fragment_node_final)
        if conn_frag:
            try:
                cursor = conn_frag.cursor(dictionary=True)
                cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                results['final_values'][fragment_node_final] = cursor.fetchone()
            except Exception as e:
                results['final_values'][fragment_node_final] = {'error': str(e)}
            finally:
                conn_frag.close()
        
        # Analysis
        successful_writers = [w for w in results['writers'].values() if w.get('success')]
        failed_writers = [w for w in results['writers'].values() if not w.get('success')]
        deadlocks = len(results['conflicts'])
        
        blocking_occurred = any(w.get('waited_for_lock', False) for w in successful_writers)
        
        # Check consistency
        final_vals = []
        for node, val in results['final_values'].items():
            if val and not isinstance(val, dict):
                final_vals.append(str(val))
            elif val and 'error' not in val:
                final_vals.append(str(val))
        
        nodes_consistent = len(set(final_vals)) <= 1 if final_vals else False
        avg_writer_duration = round(sum(w.get('duration', 0) for w in successful_writers) / len(successful_writers), 4) if successful_writers else 0
        
        return {
            'test': 'concurrent_writes',
            'test_case': 'Case #3',
            'description': 'Concurrent transactions writing on the same data item',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'nodes_involved': ['node1', fragment_node],
            'concurrent_writers': len(writer_configs),
            'note': 'Tests conflict handling (Last Write Wins, Deadlocks, or Locking)',
            'results': results,
            'analysis': {
                'successful_writes': len(successful_writers),
                'failed_writes': len(failed_writers),
                'deadlocks_detected': deadlocks,
                'blocking_occurred': blocking_occurred,
                'serialization_enforced': blocking_occurred or deadlocks > 0,
                'final_state_consistent_across_nodes': nodes_consistent,
                'average_writer_duration': avg_writer_duration,
                'explanation': self._explain_write_behavior(
                    isolation_level, len(successful_writers), deadlocks, blocking_occurred
                )
            }
        }
    
    def _explain_read_behavior(self, isolation_level, consistent):
        """Explain what's expected for concurrent reads"""
        explanations = []
        
        if isolation_level == 'READ UNCOMMITTED':
            explanations.append('READ UNCOMMITTED: Allows dirty reads, lowest isolation, highest concurrency')
        elif isolation_level == 'READ COMMITTED':
            explanations.append('READ COMMITTED: No dirty reads, but non-repeatable reads possible')
        elif isolation_level == 'REPEATABLE READ':
            explanations.append('REPEATABLE READ: No dirty or non-repeatable reads within transaction')
        else:
            explanations.append('SERIALIZABLE: Full isolation, transactions appear sequential')
        
        if consistent:
            explanations.append('✓ Data consistent across all nodes')
        else:
            explanations.append('⚠ Data inconsistent across nodes')
        
        explanations.append('Readers do not block each other (expected)')
        return ' '.join(explanations)
    
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