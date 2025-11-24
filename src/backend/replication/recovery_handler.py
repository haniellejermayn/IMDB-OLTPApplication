import logging
from datetime import datetime
from queue import Queue

logger = logging.getLogger(__name__)

class RecoveryHandler:
    def __init__(self, db_manager, transaction_logger):
        self.db = db_manager
        self.transaction_logger = transaction_logger
        self.failed_transactions_queue = Queue()
    
    def queue_failed_replication(self, node_name, operation, query, params, tconst):
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
        """Process all queued failed transactions for recovered node"""
        logger.info(f"Starting recovery for {node_name}...")
        recovered = 0
        failed = 0
        temp_queue = Queue()
        
        while not self.failed_transactions_queue.empty():
            transaction = self.failed_transactions_queue.get()
            
            if transaction['node_name'] == node_name:
                result = self.db.execute_query(
                    node_name,
                    transaction['query'],
                    transaction['params']
                )
                
                if result['success']:
                    recovered += 1
                    self.transaction_logger.log_transaction(
                        node_name,
                        f"RECOVERY_{transaction['operation']}",
                        transaction['tconst'],
                        'SUCCESS'
                    )
                    logger.info(f"✓ Recovered {transaction['operation']} for {transaction['tconst']}")
                else:
                    failed += 1
                    temp_queue.put(transaction)
                    logger.warning(f"✗ Failed to recover {transaction['operation']} for {transaction['tconst']}")
            else:
                temp_queue.put(transaction)
        
        while not temp_queue.empty():
            self.failed_transactions_queue.put(temp_queue.get())
        
        return {
            'node': node_name,
            'recovered': recovered,
            'failed': failed,
            'message': f'Recovered {recovered} transactions, {failed} still pending'
        }
    
    def get_pending_count(self):
        """Get count of pending replications"""
        return self.failed_transactions_queue.qsize()