import logging
import json
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class RecoveryHandler:
    def __init__(self, db_manager, transaction_logger):
        self.db = db_manager
        self.transaction_logger = transaction_logger
        self.retry_interval = 10  # retry every 10 seconds
        self.is_running = False
        self.retry_thread = None
    
    def start_automatic_retry(self):
        """Start background thread for automatic retry"""
        if self.is_running:
            logger.warning("Automatic retry is already running")
            return
        
        self.is_running = True
        self.retry_thread = threading.Thread(target=self._retry_loop, daemon=True)
        self.retry_thread.start()
        logger.info(f"Automatic retry started (interval: {self.retry_interval}s)")
    
    def stop_automatic_retry(self):
        """Stop background retry thread"""
        self.is_running = False
        if self.retry_thread:
            self.retry_thread.join(timeout=5)
        logger.info("Automatic retry stopped")
    
    def _retry_loop(self):
        """Background loop that retries failed replications"""
        while self.is_running:
            try:
                # Check ALL nodes for pending replications (bidirectional)
                # node1 = central, node2/node3 = fragments
                for source_node in ['node1', 'node2', 'node3']:
                    self._process_pending_replications(source_node)
                
                time.sleep(self.retry_interval)
                
            except Exception as e:
                logger.error(f"Error in retry loop: {e}")
                time.sleep(self.retry_interval)
    
    def _process_pending_replications(self, source_node):
        """Process all pending replications from a source node"""
        pending = self.transaction_logger.get_pending_replications(source_node)
        
        if not pending:
            return
        
        logger.info(f"Processing {len(pending)} pending replications from {source_node}")
        
        for transaction in pending:
            self._retry_single_transaction(source_node, transaction)
    
    def _retry_single_transaction(self, source_node, transaction):
        """Retry a single failed replication"""
        transaction_id = transaction['transaction_id']
        target_node = transaction['target_node']
        operation_type = transaction['operation_type']
        record_id = transaction['record_id']
        query = transaction['query_text']
        
        # Parse JSON params back to tuple
        try:
            params = tuple(json.loads(transaction['query_params'])) if transaction['query_params'] else ()
        except:
            params = ()
        
        logger.info(
            f"Retrying {operation_type} for {record_id}: "
            f"{source_node} → {target_node} (attempt {transaction['retry_count'] + 1})"
        )
        
        # Check if target node is online before attempting
        if not self.db.check_node(target_node):
            logger.warning(f"Target {target_node} still offline, skipping retry for {record_id}")
            return
        
        # Attempt replication
        result = self.db.execute_query(target_node, query, params)
        
        # Update retry count
        self.transaction_logger.increment_retry_count(source_node, transaction_id)
        
        if result['success']:
            self.transaction_logger.update_log_status(
                source_node, 
                transaction_id, 
                'SUCCESS'
            )
            logger.info(
                f"✓ REPLICATION SUCCESS: {operation_type} for {record_id} "
                f"({source_node} → {target_node})"
            )
        else:
            if transaction['retry_count'] + 1 >= transaction['max_retries']:
                self.transaction_logger.update_log_status(
                    source_node,
                    transaction_id,
                    'FAILED',
                    error_msg=f"Max retries reached. Last error: {result.get('error')}"
                )
                logger.error(
                    f"✗ REPLICATION FAILED PERMANENTLY: {operation_type} for {record_id} "
                    f"after {transaction['max_retries']} attempts"
                )
            else:
                logger.warning(
                    f"⚠ Retry {transaction['retry_count'] + 1} failed for {record_id}. "
                    f"Will retry again. Error: {result.get('error')}"
                )
    
    def recover_node(self, node_name):
        """
        Manual recovery: Force immediate retry of all pending replications to a node.
        """
        logger.info(f"Manual recovery triggered for {node_name}")
        
        # Check if target node is online
        if not self.db.check_node(node_name):
            return {
                'node': node_name,
                'recovered': 0,
                'failed': 0,
                'message': f'{node_name} is still offline. Cannot recover.'
            }
        
        recovered = 0
        failed = 0
        
        # Check ALL source nodes for replications targeting this node
        for source_node in ['node1', 'node2', 'node3']:
            if source_node == node_name:
                continue  # Skip self
            
            conn = self.db.get_connection(source_node)
            if not conn:
                logger.warning(f"Cannot access {source_node} for recovery check")
                continue
            
            try:
                cursor = conn.cursor(dictionary=True)
                query = """
                    SELECT * FROM transaction_log
                    WHERE target_node = %s
                      AND status = 'PENDING'
                      AND retry_count < max_retries
                    ORDER BY created_at ASC
                """
                cursor.execute(query, (node_name,))
                transactions = cursor.fetchall()
                
                logger.info(
                    f"Found {len(transactions)} pending replications "
                    f"from {source_node} to {node_name}"
                )
                
                for transaction in transactions:
                    self._retry_single_transaction(source_node, transaction)
                    
                    # Check if it succeeded
                    cursor.execute(
                        "SELECT status FROM transaction_log WHERE transaction_id = %s",
                        (transaction['transaction_id'],)
                    )
                    result = cursor.fetchone()
                    
                    if result and result['status'] == 'SUCCESS':
                        recovered += 1
                    else:
                        failed += 1
                
            except Exception as e:
                logger.error(f"Error during recovery from {source_node}: {e}")
            finally:
                conn.close()
        
        return {
            'node': node_name,
            'recovered': recovered,
            'failed': failed,
            'message': (
                f'Manual recovery complete: {recovered} transactions recovered, '
                f'{failed} still pending/failed'
            )
        }
    
    def get_pending_summary(self):
        """Get summary of pending replications across all nodes"""
        summary = {}
        total_pending = 0
        
        # Check ALL nodes (bidirectional support)
        for source_node in ['node1', 'node2', 'node3']:
            conn = self.db.get_connection(source_node)
            if not conn:
                summary[source_node] = {
                    'status': 'offline',
                    'pending_count': 0,
                    'failed_count': 0
                }
                continue
            
            try:
                cursor = conn.cursor(dictionary=True)
                
                # Count pending
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM transaction_log 
                    WHERE status = 'PENDING' AND retry_count < max_retries
                """)
                pending_count = cursor.fetchone()['count']
                
                # Count failed
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM transaction_log 
                    WHERE status = 'FAILED' OR (status = 'PENDING' AND retry_count >= max_retries)
                """)
                failed_count = cursor.fetchone()['count']
                
                # Get breakdown by target
                cursor.execute("""
                    SELECT target_node, COUNT(*) as count
                    FROM transaction_log
                    WHERE status = 'PENDING' AND retry_count < max_retries
                    GROUP BY target_node
                """)
                pending_by_target = {row['target_node']: row['count'] for row in cursor.fetchall()}
                
                summary[source_node] = {
                    'status': 'online',
                    'pending_count': pending_count,
                    'failed_count': failed_count,
                    'pending_by_target': pending_by_target
                }
                
                total_pending += pending_count
                
            except Exception as e:
                logger.error(f"Error getting summary from {source_node}: {e}")
                summary[source_node] = {
                    'status': 'error',
                    'error': str(e)
                }
            finally:
                conn.close()
        
        return {
            'total_pending': total_pending,
            'by_node': summary,
            'retry_interval': self.retry_interval,
            'automatic_retry_active': self.is_running
        }
    
    def get_pending_count(self):
        """Quick count of all pending replications"""
        summary = self.get_pending_summary()
        return summary.get('total_pending', 0)