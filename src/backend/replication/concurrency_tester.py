import logging
import threading
import time

logger = logging.getLogger(__name__)

class ConcurrencyTester:
    def __init__(self, db_manager, replication_manager):
        self.db = db_manager
        self.replication_manager = replication_manager
    
    def test_concurrent_reads(self, tconst, isolation_level='READ COMMITTED'):
        """
        Test Case 1: Concurrent reads on same data
        Multiple nodes reading simultaneously
        """
        results = {}
        threads = []
        lock = threading.Lock()
        
        # First, find out which nodes have this record
        title = self.db.get_title_by_id(tconst)
        if 'error' in title:
            return {'error': f'Title {tconst} not found'}
        
        title_type = title.get('title_type')
        
        # Determine which nodes should have this data
        # node1 has all, node2 has movies, node3 has non-movies
        if title_type == 'movie':
            nodes_to_test = ['node1', 'node2']
        else:
            nodes_to_test = ['node1', 'node3']
        
        def read_from_node(node_name):
            conn = self.db.get_connection(node_name, isolation_level)
            if conn:
                try:
                    conn.start_transaction()
                    start_time = time.time()
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    data = cursor.fetchone()
                    
                    # Simulate some read time to show concurrent behavior
                    time.sleep(0.1)
                    
                    conn.commit()
                    end_time = time.time()
                    
                    with lock:
                        results[node_name] = {
                            'success': True,
                            'data': data,
                            'read_time': round(end_time - start_time, 4),
                            'isolation_level': isolation_level
                        }
                except Exception as e:
                    conn.rollback()
                    with lock:
                        results[node_name] = {'success': False, 'error': str(e)}
                finally:
                    conn.close()
            else:
                with lock:
                    results[node_name] = {'success': False, 'error': 'Connection failed'}
        
        # Start concurrent reads
        for node in nodes_to_test:
            t = threading.Thread(target=read_from_node, args=(node,))
            threads.append(t)
            t.start()
        
        # Wait for all reads to complete
        for t in threads:
            t.join()
        
        # Check consistency across nodes that have the data
        data_values = []
        for node, result in results.items():
            if result.get('success') and result.get('data'):
                # Compare key fields
                data_values.append(str(result['data']))
        
        consistent = len(set(data_values)) <= 1 if data_values else True
        
        return {
            'test': 'concurrent_read',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'title_type': title_type,
            'nodes_tested': nodes_to_test,
            'results': results,
            'consistent': consistent,
            'explanation': 'All nodes returned the same data' if consistent else 'Data mismatch detected'
        }
    
    def test_read_write_conflict(self, tconst, new_data, isolation_level='READ COMMITTED'):
        """
        Test Case 2: One transaction writing while others are reading
        Demonstrates isolation level effects on dirty reads, etc.
        """
        results = {
            'reads_before': {},
            'write': {},
            'reads_during': {},
            'reads_after': {}
        }
        lock = threading.Lock()
        write_started = threading.Event()
        write_completed = threading.Event()
        
        # Get original value
        original = self.db.get_title_by_id(tconst)
        if 'error' in original:
            return {'error': f'Title {tconst} not found'}
        
        title_type = original.get('title_type')
        primary_node = 'node2' if title_type == 'movie' else 'node3'
        
        def read_during_write(node_name, result_key):
            """Read that happens while write is in progress"""
            # Wait for write to start
            write_started.wait(timeout=5)
            
            conn = self.db.get_connection(node_name, isolation_level)
            if conn:
                try:
                    conn.start_transaction()
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    data = cursor.fetchone()
                    conn.commit()
                    
                    with lock:
                        results[result_key][node_name] = {
                            'success': True,
                            'data': data,
                            'isolation_level': isolation_level
                        }
                except Exception as e:
                    conn.rollback()
                    with lock:
                        results[result_key][node_name] = {'success': False, 'error': str(e)}
                finally:
                    conn.close()
        
        def do_write():
            """Perform the write operation"""
            conn = self.db.get_connection(primary_node, isolation_level)
            if conn:
                try:
                    conn.start_transaction()
                    
                    # Build update query
                    set_clauses = []
                    params = []
                    for key, value in new_data.items():
                        if key != 'tconst':
                            set_clauses.append(f"{key} = %s")
                            params.append(value)
                    params.append(tconst)
                    
                    cursor = conn.cursor()
                    query = f"UPDATE titles SET {', '.join(set_clauses)} WHERE tconst = %s"
                    
                    # Signal that write has started
                    write_started.set()
                    
                    cursor.execute(query, tuple(params))
                    
                    # Hold the transaction open briefly so readers can see uncommitted state
                    time.sleep(0.3)
                    
                    conn.commit()
                    write_completed.set()
                    
                    with lock:
                        results['write'] = {
                            'success': True,
                            'node': primary_node,
                            'query': query
                        }
                except Exception as e:
                    conn.rollback()
                    write_started.set()
                    write_completed.set()
                    with lock:
                        results['write'] = {'success': False, 'error': str(e)}
                finally:
                    conn.close()
        
        # Record state before
        results['reads_before']['original'] = original
        
        # Start concurrent readers and writer
        threads = []
        
        # Writer thread
        writer = threading.Thread(target=do_write)
        threads.append(writer)
        
        # Reader threads (read during write)
        for node in ['node1', primary_node]:
            t = threading.Thread(target=read_during_write, args=(node, 'reads_during'))
            threads.append(t)
        
        # Start all
        for t in threads:
            t.start()
        
        # Wait for all to complete
        for t in threads:
            t.join()
        
        # Read after write completes
        write_completed.wait(timeout=5)
        time.sleep(0.1)  # Small delay to ensure commit propagates
        
        for node in ['node1', primary_node]:
            conn = self.db.get_connection(node, isolation_level)
            if conn:
                try:
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    results['reads_after'][node] = cursor.fetchone()
                except Exception as e:
                    results['reads_after'][node] = {'error': str(e)}
                finally:
                    conn.close()
        
        # Analyze results
        analysis = self._analyze_read_write_results(results, new_data, isolation_level)
        
        return {
            'test': 'read_write_conflict',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'new_data': new_data,
            'results': results,
            'analysis': analysis
        }
    
    def _analyze_read_write_results(self, results, new_data, isolation_level):
        """Analyze read-write conflict test results"""
        analysis = {
            'dirty_read_possible': False,
            'explanation': ''
        }
        
        # Check if any reads during write saw the new value
        for node, read_result in results.get('reads_during', {}).items():
            if read_result.get('success') and read_result.get('data'):
                for key, new_val in new_data.items():
                    if key in read_result['data'] and read_result['data'][key] == new_val:
                        if isolation_level == 'READ UNCOMMITTED':
                            analysis['dirty_read_possible'] = True
                            analysis['explanation'] = 'READ UNCOMMITTED allows seeing uncommitted changes (dirty reads)'
        
        if isolation_level == 'READ UNCOMMITTED':
            analysis['explanation'] = 'READ UNCOMMITTED: May see uncommitted data (dirty reads possible)'
        elif isolation_level == 'READ COMMITTED':
            analysis['explanation'] = 'READ COMMITTED: Only sees committed data, no dirty reads'
        elif isolation_level == 'REPEATABLE READ':
            analysis['explanation'] = 'REPEATABLE READ: Same read returns same result within transaction'
        elif isolation_level == 'SERIALIZABLE':
            analysis['explanation'] = 'SERIALIZABLE: Full isolation, transactions appear sequential'
        
        return analysis
    
    def test_concurrent_writes(self, updates, isolation_level='READ COMMITTED'):
        """
        Test Case 3: Concurrent writes
        Multiple updates happening simultaneously on same/different data
        """
        results = {}
        threads = []
        lock = threading.Lock()
        
        def write_to_nodes(update_data, index):
            tconst = update_data['tconst']
            new_data = update_data['data']
            
            try:
                start_time = time.time()
                result = self.replication_manager.update_title(tconst, new_data, isolation_level)
                end_time = time.time()
                
                with lock:
                    results[f'update_{index}_{tconst}'] = {
                        'success': result['success'],
                        'write_time': round(end_time - start_time, 4),
                        'tconst': tconst,
                        'new_data': new_data,
                        'result': result
                    }
            except Exception as e:
                with lock:
                    results[f'update_{index}_{tconst}'] = {'success': False, 'error': str(e)}
        
        # Start concurrent writes
        for i, update in enumerate(updates):
            t = threading.Thread(target=write_to_nodes, args=(update, i))
            threads.append(t)
            t.start()
        
        # Wait for all writes to complete
        for t in threads:
            t.join()
        
        # Verify final state
        final_states = {}
        for update in updates:
            tconst = update['tconst']
            final_states[tconst] = self.db.get_title_by_id(tconst)
        
        return {
            'test': 'concurrent_write',
            'isolation_level': isolation_level,
            'updates_attempted': len(updates),
            'results': results,
            'final_states': final_states
        }
    
    def simulate_failure(self, scenario):
        """
        Guide for simulating failure scenarios
        """
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