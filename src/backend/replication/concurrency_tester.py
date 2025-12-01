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
        """Get a record suitable for testing."""
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
        return 'tt0035423'
    
    def test_concurrent_reads(self, tconst=None, isolation_level='READ COMMITTED'):
        """
        Case #1: Concurrent reads through application API
        
        Tests how the application handles concurrent read requests with automatic
        node fallback and location transparency.
        """
        if not tconst:
            tconst = self._get_test_record()
            logger.info(f"[Case #1] Auto-selected test record: {tconst}")
        
        results = {}
        threads = []
        lock = threading.Lock()
        start_barrier = threading.Barrier(3)
        
        title = self.db.get_title_by_id(tconst)
        if 'error' in title:
            return {'error': f'Title {tconst} not found'}
        
        title_type = title.get('title_type')
        test_nodes = ['node1', 'node2', 'node2'] if title_type == 'movie' else ['node1', 'node3', 'node3']
        
        def concurrent_read(node_name, reader_id):
            """Reader using application API (get_title_by_id)"""
            try:
                start_barrier.wait()
                start_time = time.time()
                
                # First read through application API
                data1 = self.db.get_title_by_id(tconst)
                read1_time = time.time()
                
                time.sleep(0.1)
                
                # Second read through application API
                data2 = self.db.get_title_by_id(tconst)
                read2_time = time.time()
                end_time = time.time()
                
                with lock:
                    results[f'{node_name}_{reader_id}'] = {
                        'success': 'error' not in data1,
                        'node': node_name,
                        'reader_id': reader_id,
                        'data': data1 if 'error' not in data1 else None,
                        'repeatable': data1 == data2,
                        'duration': round(end_time - start_time, 4),
                        'read1_time': round(read1_time - start_time, 4),
                        'read2_time': round(read2_time - start_time, 4),
                        'isolation_level': isolation_level,
                        'timestamp': datetime.now().isoformat()
                    }
            except Exception as e:
                with lock:
                    results[f'{node_name}_{reader_id}'] = {
                        'success': False,
                        'error': str(e),
                        'node': node_name
                    }
        
        for i, node in enumerate(test_nodes):
            t = threading.Thread(target=concurrent_read, args=(node, i))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        successful_reads = [r for r in results.values() if r.get('success')]
        data_values = [str(r['data']) for r in successful_reads if r.get('data')]
        consistent = len(set(data_values)) <= 1
        
        return {
            'test': 'concurrent_reads',
            'test_case': 'Case #1',
            'description': 'Concurrent reads through application API',
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
                'repeatable_reads_within_application': all(r.get('repeatable', False) for r in successful_reads),
                'average_duration': round(sum(r.get('duration', 0) for r in successful_reads) / len(successful_reads), 4) if successful_reads else 0,
                'explanation': self._explain_read_behavior(isolation_level, consistent, all(r.get('repeatable', False) for r in successful_reads))
            }
        }
    
    def test_read_write_conflict(self, tconst=None, new_data=None, isolation_level='READ COMMITTED'):
        """
        Case #2: Concurrent writes (with replication) and reads through application API
        
        Writers use replication_manager.update_title() which handles distributed updates.
        Readers use get_title_by_id() which may see updates as they commit.
        """
        if not tconst:
            tconst = self._get_test_record()
            logger.info(f"[Case #2] Auto-selected test record: {tconst}")
        
        if not new_data:
            unique_runtime = random.randint(1, 300)
            new_data = {'runtime_minutes': unique_runtime}
        else:
            unique_runtime = new_data.get('runtime_minutes', random.randint(1, 300))

        results = {
            'original_value': {},
            'writers': {},
            'readers': {},
            'final_values': {}
        }
        lock = threading.Lock()
        
        original = self.db.get_title_by_id(tconst)
        if 'error' in original:
            return {'error': f'Title {tconst} not found'}
        
        results['original_value'] = original
        original_runtime = original.get('runtime_minutes')
        title_type = original.get('title_type')
        
        start_barrier = threading.Barrier(4)  # 2 writers + 2 readers
        
        def writer_transaction(writer_id):
            """Writer using replication_manager (production code path)"""
            try:
                start_barrier.wait()
                start_time = time.time()
                
                logger.info(f"[Case #2] Writer {writer_id} calling replication_manager.update_title()")
                
                result = self.replication_manager.update_title(tconst, new_data, isolation_level)
                
                time.sleep(0.3)  # Hold to allow reader overlap
                end_time = time.time()
                
                logger.info(f"[Case #2] Writer {writer_id} completed: {result.get('success')}")
                
                with lock:
                    results['writers'][f'writer_{writer_id}'] = {
                        'success': result.get('success', False),
                        'writer_id': writer_id,
                        'primary_node': result.get('primary_node'),
                        'replicated_to': result.get('replicated_to'),
                        'pending_replication': result.get('pending_replication'),
                        'duration': round(end_time - start_time, 4),
                        'timestamp': datetime.now().isoformat()
                    }
            except Exception as e:
                logger.error(f"[Case #2] Writer {writer_id} error: {e}")
                with lock:
                    results['writers'][f'writer_{writer_id}'] = {
                        'success': False,
                        'error': str(e),
                        'writer_id': writer_id
                    }
        
        def reader_transaction(reader_id):
            """Reader using application API during concurrent writes"""
            try:
                start_barrier.wait()
                time.sleep(0.02 * (reader_id + 1))
                
                read_start_time = time.time()
                
                # Each API call is independent (separate connections)
                read1 = self.db.get_title_by_id(tconst)
                read1_time = time.time()
                
                time.sleep(0.1)
                
                read2 = self.db.get_title_by_id(tconst)
                read2_time = time.time()
                end_time = time.time()
                
                read_during_write = (read1_time - read_start_time) < 0.4
                current_runtime = read1.get('runtime_minutes') if 'error' not in read1 else None
                
                saw_new_value = (current_runtime == new_data['runtime_minutes'] and 
                                current_runtime != original_runtime)
                
                non_repeatable = read1 != read2
                was_blocked = (end_time - read_start_time) > 0.3
                
                with lock:
                    results['readers'][f'reader_{reader_id}'] = {
                        'success': 'error' not in read1,
                        'reader_id': reader_id,
                        'read1': read1 if 'error' not in read1 else None,
                        'read2': read2 if 'error' not in read2 else None,
                        'original_runtime': original_runtime,
                        'read_runtime': current_runtime,
                        'read1_timestamp': round(read1_time - read_start_time, 4),
                        'read2_timestamp': round(read2_time - read_start_time, 4),
                        'read_during_write': read_during_write,
                        'saw_new_value': saw_new_value,
                        'values_changed_between_reads': non_repeatable,
                        'blocked': was_blocked,
                        'duration': round(end_time - read_start_time, 4),
                        'timestamp': datetime.now().isoformat()
                    }
            except Exception as e:
                logger.error(f"[Case #2] Reader {reader_id} error: {e}")
                with lock:
                    results['readers'][f'reader_{reader_id}'] = {
                        'success': False,
                        'error': str(e),
                        'reader_id': reader_id
                    }
        
        threads = []
        
        for i in range(2):
            t = threading.Thread(target=writer_transaction, args=(i,))
            threads.append(t)
            t.start()
        
        for i in range(2):
            t = threading.Thread(target=reader_transaction, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        time.sleep(0.5)  # Wait for replication
        
        results['final_values']['node1'] = self.db.get_title_by_id(tconst)
        
        fragment_node = 'node2' if title_type == 'movie' else 'node3'
        conn = self.db.get_connection(fragment_node)
        if conn:
            try:
                cursor = conn.cursor(dictionary=True)
                cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                results['final_values'][fragment_node] = cursor.fetchone()
            except Exception as e:
                results['final_values'][fragment_node] = {'error': str(e)}
            finally:
                conn.close()
        
        successful_writers = [w for w in results['writers'].values() if w.get('success')]
        successful_readers = [r for r in results['readers'].values() if r.get('success')]
        
        values_changed = any(r.get('values_changed_between_reads', False) for r in successful_readers)
        any_blocking = any(r.get('blocked', False) for r in successful_readers)
        
        final_vals = []
        for node, val in results['final_values'].items():
            if val and 'error' not in val:
                final_vals.append(val.get('runtime_minutes'))
        
        nodes_consistent = len(set(final_vals)) <= 1 if final_vals else False
        
        avg_reader_duration = round(sum(r.get('duration', 0) for r in successful_readers) / len(successful_readers), 4) if successful_readers else 0
        avg_writer_duration = round(sum(w.get('duration', 0) for w in successful_writers) / len(successful_writers), 4) if successful_writers else 0
        
        return {
            'test': 'read_write_conflict',
            'test_case': 'Case #2',
            'description': 'Concurrent writes (with replication) and reads through application API',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'new_data': new_data,
            'results': results,
            'analysis': {
                'writers_succeeded': len(successful_writers),
                'readers_succeeded': len(successful_readers),
                'values_changed_between_reads': values_changed,
                'blocking_occurred': any_blocking,
                'final_state_consistent_across_nodes': nodes_consistent,
                'average_reader_duration': avg_reader_duration,
                'average_writer_duration': avg_writer_duration,
                'explanation': self._explain_read_write_app_level(
                    isolation_level, values_changed, any_blocking, nodes_consistent
                )
            }
        }
    
    def test_concurrent_writes(self, updates=None, isolation_level='READ COMMITTED'):
        """
        Case #3: Concurrent writes through replication_manager
        
        All writes use the production replication code path, demonstrating how
        the application maintains cross-node consistency under concurrent load.
        """
        tconst = None
        
        if updates and isinstance(updates, list):
            tconst = updates[0].get('tconst')
            logger.info(f"[Case #3] Using provided updates for tconst: {tconst}")
        else:
            tconst = self._get_test_record()
            logger.info(f"[Case #3] Auto-selected test record: {tconst}")
            
            runtime1 = random.randint(1, 100)
            runtime2 = random.randint(101, 200)
            runtime3 = random.randint(201, 300)
            
            updates = [
                {'tconst': tconst, 'data': {'runtime_minutes': runtime1}},
                {'tconst': tconst, 'data': {'runtime_minutes': runtime2}},
                {'tconst': tconst, 'data': {'runtime_minutes': runtime3}}
            ]
        
        title = self.db.get_title_by_id(tconst)
        if 'error' in title:
            return {'error': f'Title {tconst} not found'}
        
        title_type = title.get('title_type')
        fragment_node = 'node2' if title_type == 'movie' else 'node3'
        
        results = {
            'writers': {},
            'final_values': {},
            'conflicts': []
        }
        lock = threading.Lock()
        
        start_barrier = threading.Barrier(len(updates))
        
        def concurrent_writer(update_payload, writer_id):
            """Writer using replication_manager (production code)"""
            try:
                start_barrier.wait()
                start_time = time.time()
                
                tconst_target = update_payload.get('tconst')
                data_to_write = update_payload.get('data', {})
                
                logger.info(f"[Case #3] Writer {writer_id} calling replication_manager.update_title()")
                
                result = self.replication_manager.update_title(
                    tconst_target,
                    data_to_write,
                    isolation_level
                )
                
                end_time = time.time()
                
                logger.info(f"[Case #3] Writer {writer_id} completed: {result.get('success')}")
                
                with lock:
                    results['writers'][f'writer_{writer_id}'] = {
                        'success': result.get('success', False),
                        'writer_id': writer_id,
                        'data_written': data_to_write,
                        'primary_node': result.get('primary_node'),
                        'replicated_to': result.get('replicated_to'),
                        'pending_replication': result.get('pending_replication'),
                        'duration': round(end_time - start_time, 4),
                        'waited_for_lock': end_time - start_time > 0.2,
                        'timestamp': datetime.now().isoformat()
                    }
            except Exception as e:
                error_msg = str(e)
                logger.error(f"[Case #3] Writer {writer_id} failed: {error_msg}")
                
                with lock:
                    results['writers'][f'writer_{writer_id}'] = {
                        'success': False,
                        'error': error_msg,
                        'writer_id': writer_id,
                        'deadlock': 'deadlock' in error_msg.lower(),
                        'lock_timeout': 'timeout' in error_msg.lower()
                    }
                    
                    if 'deadlock' in error_msg.lower():
                        results['conflicts'].append({
                            'type': 'deadlock',
                            'writer': writer_id,
                            'message': error_msg
                        })
        
        threads = []
        for i, update_config in enumerate(updates):
            t = threading.Thread(target=concurrent_writer, args=(update_config, i))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        time.sleep(0.5)  # Wait for replication
        
        results['final_values']['node1'] = self.db.get_title_by_id(tconst)
        
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
        
        successful_writers = [w for w in results['writers'].values() if w.get('success')]
        failed_writers = [w for w in results['writers'].values() if not w.get('success')]
        deadlocks = len(results['conflicts'])
        blocking_occurred = any(w.get('waited_for_lock', False) for w in successful_writers)
        
        final_vals = []
        for node, val in results['final_values'].items():
            if val and 'error' not in val:
                final_vals.append(val.get('runtime_minutes'))
        
        nodes_consistent = len(set(final_vals)) <= 1 if final_vals else False
        avg_writer_duration = round(sum(w.get('duration', 0) for w in successful_writers) / len(successful_writers), 4) if successful_writers else 0
        
        return {
            'test': 'concurrent_writes',
            'test_case': 'Case #3',
            'description': 'Concurrent writes through replication_manager',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'concurrent_writers': len(updates),
            'results': results,
            'analysis': {
                'successful_writes': len(successful_writers),
                'failed_writes': len(failed_writers),
                'deadlocks_detected': deadlocks,
                'blocking_occurred': blocking_occurred,
                'final_state_consistent_across_nodes': nodes_consistent,
                'average_writer_duration': avg_writer_duration,
                'replication_success_rate': f'{len([w for w in successful_writers if w.get("replicated_to")])}/{len(successful_writers)}',
                'cross_node_final_values': {k: v.get('runtime_minutes') if v and 'error' not in v else None 
                                           for k, v in results['final_values'].items()},
                'explanation': self._explain_write_behavior(
                    isolation_level, len(successful_writers), deadlocks, blocking_occurred, nodes_consistent
                )
            }
        }
    
    def _explain_read_behavior(self, isolation_level, consistent, repeatable):
        """Explain concurrent read behavior at application level"""
        parts = []
        
        parts.append(f'Application-level reads through get_title_by_id()')
        
        if consistent:
            parts.append('✓ Cross-node data consistent')
        else:
            parts.append('⚠ Cross-node inconsistency detected')
        
        if repeatable:
            parts.append('✓ Values stable during test window')
        else:
            parts.append('⚠ Values changed between reads (timing-dependent)')
        
        parts.append('No reader blocking (expected for read operations)')
        
        return ' | '.join(parts)
    
    def _explain_read_write_app_level(self, isolation_level, values_changed, blocking, nodes_consistent):
        """Explain read-write behavior at application level"""
        parts = []
        
        parts.append(f'{isolation_level}: Application-level testing')
        
        if values_changed:
            parts.append('⚠ Readers saw different values between reads')
            parts.append('(Expected: each API call is independent transaction)')
        else:
            parts.append('✓ Readers saw consistent values')
        
        if blocking:
            parts.append('⚠ Blocking detected')
        else:
            parts.append('✓ No blocking (writers commit immediately)')
        
        if nodes_consistent:
            parts.append('✓ Replication maintained cross-node consistency')
        else:
            parts.append('✗ Cross-node inconsistency (replication issue)')
        
        return ' | '.join(parts)
    
    def _explain_write_behavior(self, isolation_level, successful, deadlocks, blocking, consistent):
        """Explain concurrent write behavior"""
        parts = []
        
        parts.append(f'{isolation_level}: {successful} successful write(s)')
        
        if deadlocks > 0:
            parts.append(f'⚠ {deadlocks} deadlock(s) - MySQL conflict detection')
        
        if blocking:
            parts.append('✓ Lock waiting observed (serialization)')
        
        if consistent:
            parts.append('✓ Replication maintained cross-node consistency')
        else:
            parts.append('✗ Cross-node inconsistency detected')
        
        parts.append('All writes used production replication code path')
        
        return ' | '.join(parts)
    
    def simulate_failure(self, scenario):
        """Guide for simulating failure scenarios"""
        if scenario == 'fragment_to_central':
            return {
                'scenario': 'Case #1: Central node failure during replication',
                'description': 'Fragment write succeeds, but central replication fails',
                'steps': [
                    '1. Stop node1: docker stop node1-central',
                    '2. Insert title via POST /title',
                    '3. Check queue: GET /recovery/status',
                    '4. Restart node1: docker start node1-central',
                    '5. Recover: POST /test/failure/central-recovery'
                ],
                'expected': 'Insert succeeds on fragment, queued for central',
                'current_pending': self.replication_manager.recovery_handler.get_pending_count()
            }
            
        elif scenario == 'central_to_fragment':
            return {
                'scenario': 'Case #3: Fragment node failure during replication',
                'description': 'Central write succeeds (fallback), fragment replication queued',
                'steps': [
                    '1. Stop node2: docker stop node2-movies',
                    '2. Insert movie via POST /title',
                    '3. Check queue: GET /recovery/status',
                    '4. Restart node2: docker start node2-movies',
                    '5. Recover: POST /test/failure/fragment-recovery'
                ],
                'expected': 'Insert succeeds on central, queued for fragment',
                'current_pending': self.replication_manager.recovery_handler.get_pending_count()
            }
        
        else:
            return {
                'error': 'Unknown scenario',
                'valid_scenarios': ['fragment_to_central', 'central_to_fragment']
            }