import logging
from datetime import datetime
import threading
import time
from queue import Queue

logger = logging.getLogger(__name__)

class ReplicationManager:
    def __init__(self, db_manager):
        self.db = db_manager
        self.failed_transactions_queue = Queue() # queue for failed replications
        
    def log_transaction(self, node_id, operation_type, record_id, table_name='titles', replicated=False):
        query = """
            INSERT INTO transaction_log (transaction_id, node_id, operation_type, table_name, record_id, replicated)
            VALUES (UUID(), %s, %s, %s, %s, %s)
        """
        result = self.db.execute_query(
            'node1',
            query,
            (node_id, operation_type, table_name, record_id, replicated)
        )
        if not result['success']:
            logger.error(f"Failed to log transaction: {result.get('error')}")

    
    def insert_title(self, data):
        """Insert title with replication to appropriate nodes"""
        tconst = data.get('tconst')
        title_type = data.get('title_type')
        
        # node2 if movie, node3 if non-movie
        target_node = 'node2' if title_type == 'movie' else 'node3'
        
        query = """
            INSERT INTO titles (tconst, title_type, primary_title, start_year, runtime_minutes, genres)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (
            tconst,
            title_type,
            data.get('primary_title'),
            data.get('start_year'),
            data.get('runtime_minutes'),
            data.get('genres')
        )
        
        results = {}
        
        # master-slave approach: fragment first then central
        # insert to fragment
        result_frag = self.db.execute_query(target_node, query, params)
        results[target_node] = result_frag
        
        if result_frag['success']:
            self.log_transaction(target_node, 'INSERT', tconst, 'SUCCESS', None)
            
            # replicate to central
            result1 = self.db.execute_query('node1', query, params)
            results['node1'] = result1
            
            if result1['success']:
                self.log_transaction('node1', 'INSERT', tconst, 'SUCCESS', None)
            else:
                # Case #1: Failed to replicate to central
                self.log_transaction('node1', 'INSERT', tconst, 'FAILED', result1.get('error'))
                self._queue_failed_replication('node1', 'INSERT', query, params, tconst)
                logger.warning(f"Failed to replicate INSERT to node1 for {tconst}, queued for retry")
        else:
            self.log_transaction(target_node, 'INSERT', tconst, 'FAILED', result_frag.get('error'))
        
        return {
            'success': result_frag['success'],
            'replicated_to_central': results.get('node1', {}).get('success', False),
            'results': results
        }
    
    def update_title(self, tconst, data, isolation_level='READ COMMITTED'):
        """Update title with replication"""
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        target_node = 'node2' if title_type == 'movie' else 'node3'
        
        # update query
        set_clauses = []
        params = []
        
        for key, value in data.items():
            if key != 'tconst':
                set_clauses.append(f"{key} = %s")
                params.append(value)
        
        params.append(tconst)
        
        query = f"UPDATE titles SET {', '.join(set_clauses)} WHERE tconst = %s"
        
        results = {}
        
        # update central first (node1)
        result1 = self.db.execute_query('node1', query, params, isolation_level)
        results['node1'] = result1
        
        if result1['success']:
            self.log_transaction('node1', 'UPDATE', tconst, 'SUCCESS', None)
            
            # replicate to fragment
            result_frag = self.db.execute_query(target_node, query, params, isolation_level)
            results[target_node] = result_frag
            
            if result_frag['success']:
                self.log_transaction(target_node, 'UPDATE', tconst, 'SUCCESS', None)
            else:
                # Case #3: Failed to replicate to fragment node
                self.log_transaction(target_node, 'UPDATE', tconst, 'FAILED', result_frag.get('error'))
                self._queue_failed_replication(target_node, 'UPDATE', query, params, tconst)
                logger.warning(f"Failed to replicate UPDATE to {target_node} for {tconst}, queued for retry")
        else:
            self.log_transaction('node1', 'UPDATE', tconst, 'FAILED', result1.get('error'))
        
        return {
            'success': result1['success'],
            'replicated_to_fragment': results.get(target_node, {}).get('success', False),
            'results': results
        }
    
    def delete_title(self, tconst):
        """Delete title with replication"""
        title = self.db.get_title_by_id(tconst)
        
        if 'error' in title:
            return {'success': False, 'error': 'Title not found'}
        
        title_type = title['title_type']
        target_node = 'node2' if title_type == 'movie' else 'node3'
        
        query = "DELETE FROM titles WHERE tconst = %s"
        
        results = {}
        
        # delete from central first (node1)
        result1 = self.db.execute_query('node1', query, (tconst,))
        results['node1'] = result1
        
        if result1['success']:
            self.log_transaction('node1', 'DELETE', tconst, 'SUCCESS', None)
            
            # replicate deletion to fragment
            result_frag = self.db.execute_query(target_node, query, (tconst,))
            results[target_node] = result_frag
            
            if result_frag['success']:
                self.log_transaction(target_node, 'DELETE', tconst, 'SUCCESS', None)
            else:
                self.log_transaction(target_node, 'DELETE', tconst, 'FAILED', result_frag.get('error'))
                self._queue_failed_replication(target_node, 'DELETE', query, (tconst,), tconst)
        else:
            self.log_transaction('node1', 'DELETE', tconst, 'FAILED', result1.get('error'))
        
        return {
            'success': result1['success'],
            'replicated_to_fragment': results.get(target_node, {}).get('success', False),
            'results': results
        }
    
    def _queue_failed_replication(self, node_name, operation, query, params, tconst):
        """Queue failed replication for retry"""
        self.failed_transactions_queue.put({
            'node_name': node_name,
            'operation': operation,
            'query': query,
            'params': params,
            'tconst': tconst,
            'timestamp': datetime.now()
        })
    
    def recover_node(self, node_name):
        """
        Case #2 & #4: Recover node from missed transactions
        Process all queued failed transactions for the recovered node
        """
        logger.info(f"Starting recovery for {node_name}...")
        recovered = 0
        failed = 0
        
        temp_queue = Queue()
        
        # process all items in queue
        while not self.failed_transactions_queue.empty():
            transaction = self.failed_transactions_queue.get()
            
            if transaction['node_name'] == node_name:
                # try to replay this transaction
                result = self.db.execute_query(
                    node_name,
                    transaction['query'],
                    transaction['params']
                )
                
                if result['success']:
                    recovered += 1
                    self.log_transaction(
                        node_name,
                        f"RECOVERY_{transaction['operation']}",
                        transaction['tconst'],
                        'SUCCESS',
                        'Recovered after node failure'
                    )
                    logger.info(f"✓ Recovered {transaction['operation']} for {transaction['tconst']}")
                else:
                    failed += 1
                    temp_queue.put(transaction)  # re-queue if still failing
                    logger.warning(f"✗ Failed to recover {transaction['operation']} for {transaction['tconst']}")
            else:
                temp_queue.put(transaction)  # keep transactions for other nodes
        
        # restore unprocessed transactions back to main queue
        while not temp_queue.empty():
            self.failed_transactions_queue.put(temp_queue.get())
        
        return {
            'node': node_name,
            'recovered': recovered,
            'failed': failed,
            'message': f'Recovered {recovered} transactions, {failed} still pending'
        }
    
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
    
    def get_pending_replications(self):
        """Get count of pending failed replications"""
        return {
            'pending_count': self.failed_transactions_queue.qsize(),
            'message': 'Transactions waiting for node recovery'
        }