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
        
        def read_from_node(node_name):
            conn = self.db.get_connection(node_name, isolation_level)
            if conn:
                try:
                    start_time = time.time()
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute("SELECT * FROM titles WHERE tconst = %s", (tconst,))
                    data = cursor.fetchone()
                    end_time = time.time()
                    
                    with lock:
                        results[node_name] = {
                            'success': True,
                            'data': data,
                            'read_time': end_time - start_time,
                            'isolation_level': isolation_level
                        }
                except Exception as e:
                    with lock:
                        results[node_name] = {'success': False, 'error': str(e)}
                finally:
                    conn.close()
            else:
                with lock:
                    results[node_name] = {'success': False, 'error': 'Connection failed'}
        
        # start concurrent reads
        for node in ['node1', 'node2', 'node3']:
            t = threading.Thread(target=read_from_node, args=(node,))
            threads.append(t)
            t.start()
        
        # wait for all reads to complete
        for t in threads:
            t.join()
        
        # check consistency
        data_values = [r['data'] for r in results.values() if r.get('success') and r.get('data')]
        consistent = len(set(str(d) for d in data_values)) <= 1 if data_values else True
        
        return {
            'test': 'concurrent_read',
            'isolation_level': isolation_level,
            'tconst': tconst,
            'results': results,
            'consistent': consistent
        }
    
    def test_concurrent_writes(self, updates, isolation_level='READ COMMITTED'):
        """
        Test Case 2 & 3: Concurrent writes
        Multiple updates happening simultaneously on same/different data
        """
        results = {}
        threads = []
        lock = threading.Lock()
        
        def write_to_nodes(update_data):
            tconst = update_data['tconst']
            new_data = update_data['data']
            
            try:
                start_time = time.time()
                result = self.update_title(tconst, new_data, isolation_level)
                end_time = time.time()
                
                with lock:
                    results[tconst] = {
                        'success': result['success'],
                        'write_time': end_time - start_time,
                        'results': result['results']
                    }
            except Exception as e:
                with lock:
                    results[tconst] = {'success': False, 'error': str(e)}
        
        # start concurrent writes
        for update in updates:
            t = threading.Thread(target=write_to_nodes, args=(update,))
            threads.append(t)
            t.start()
        
        # wait for all writes to complete
        for t in threads:
            t.join()
        
        return {
            'test': 'concurrent_write',
            'isolation_level': isolation_level,
            'results': results
        }
    
    def simulate_failure(self, scenario):
        """
        Simulate node failure scenarios
        
        Scenarios:
        - 'fragment_to_central': Simulate Case #1
        - 'central_recovery': Simulate Case #2
        - 'central_to_fragment': Simulate Case #3
        - 'fragment_recovery': Simulate Case #4
        """
        if scenario == 'fragment_to_central':
            # Case #1: Insert to fragment succeeds, replication to central fails
            return self._simulate_case1()
        
        elif scenario == 'central_recovery':
            # Case #2: Central node recovers and catches up
            return self.recover_node('node1')
        
        elif scenario == 'central_to_fragment':
            # Case #3: Update on central succeeds, replication to fragment fails
            return self._simulate_case3()
        
        elif scenario == 'fragment_recovery':
            # Case #4: Fragment node recovers
            node = 'node2'  # or 'node3'
            return self.recover_node(node)
        
        else:
            return {'error': 'Unknown scenario', 'valid_scenarios': [
                'fragment_to_central', 'central_recovery',
                'central_to_fragment', 'fragment_recovery'
            ]}
            
    def _simulate_case1(self):
        """Case #1: Fragment write succeeds, central replication fails"""
        # This is already handled in insert_title() method
        # When central node is down, transaction gets queued
        return {
            'scenario': 'Case #1',
            'description': 'Fragment write succeeds, but central replication fails',
            'behavior': 'Transaction queued for recovery when central node comes back online',
            'queue_size': self.failed_transactions_queue.qsize()
        }
    
    def _simulate_case3(self):
        """Case #3: Central write succeeds, fragment replication fails"""
        # This is already handled in update_title() method
        return {
            'scenario': 'Case #3',
            'description': 'Central write succeeds, but fragment replication fails',
            'behavior': 'Transaction queued for recovery when fragment node comes back online',
            'queue_size': self.failed_transactions_queue.qsize()
        }