import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class TransactionLogger:
    def __init__(self, db_manager):
        self.db = db_manager
    
    def log_replication(self, source_node, target_node, operation_type, 
                       record_id, query, params, status='PENDING', error_msg=None):
        """
        Log a replication operation to the source node's transaction_log.
        
        CRITICAL: This logs to the node where the write SUCCEEDED (source_node).
        That node must be online when calling this method.
        
        Args:
            source_node: node where write succeeded (node1/node2/node3)
            target_node: node where replica should go
            operation_type: INSERT/UPDATE/DELETE
            record_id: tconst value
            query: SQL query text
            params: Query parameters
            status: SUCCESS/FAILED/PENDING
            error_msg: Error message if failed
            
        Returns:
            transaction_id or None if logging failed
        """
        transaction_id = self._generate_transaction_id()
        
        log_query = """
            INSERT INTO transaction_log 
            (transaction_id, source_node, target_node, operation_type, 
             table_name, record_id, status, query_text, query_params, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Convert params tuple to JSON
        params_json = json.dumps(list(params)) if params else None
        
        log_params = (
            transaction_id,
            source_node,
            target_node,
            operation_type,
            'titles',
            record_id,
            status,
            query,
            params_json,
            error_msg
        )
        
        # CRITICAL: Log to SOURCE node (the one that succeeded)
        # If source node is down, we can't log - but that's OK because
        # it means the write itself failed, so there's nothing to replicate
        result = self.db.execute_query(source_node, log_query, log_params)
        
        if not result['success']:
            logger.error(
                f"CRITICAL: Failed to log transaction on {source_node}! "
                f"Operation {operation_type} on {record_id} may be lost. "
                f"Error: {result.get('error')}"
            )
            
            # IMPROVEMENT: Try to log to target node as well (belt and suspenders)
            logger.warning(f"Attempting to log to {target_node} as backup...")
            backup_result = self.db.execute_query(target_node, log_query, log_params)
            
            if backup_result['success']:
                logger.info(f"✓ Successfully logged to {target_node} as backup")
                return transaction_id
            else:
                logger.error(f"✗ Backup logging to {target_node} also failed!")
                return None
        
        logger.info(
            f"Logged {operation_type} for {record_id}: "
            f"{source_node} → {target_node} (status: {status})"
        )
        return transaction_id
    
    def update_log_status(self, node, transaction_id, status, error_msg=None):
        """
        Update status of a logged transaction.
        
        Args:
            node: The node where the log entry exists
            transaction_id: Transaction ID to update
            status: New status (SUCCESS/FAILED/PENDING)
            error_msg: Optional error message
        """
        query = """
            UPDATE transaction_log 
            SET status = %s, 
                error_message = %s,
                completed_at = NOW(),
                last_retry_at = NOW()
            WHERE transaction_id = %s
        """
        
        result = self.db.execute_query(node, query, (status, error_msg, transaction_id))
        
        if result['success']:
            logger.info(f"✓ Updated transaction {transaction_id} to {status} on {node}")
        else:
            logger.error(f"✗ Failed to update transaction {transaction_id} on {node}")
        
        return result
    
    def increment_retry_count(self, node, transaction_id):
        """
        Increment retry counter for a pending transaction.
        
        Args:
            node: Node where the log entry exists
            transaction_id: Transaction ID to update
        """
        query = """
            UPDATE transaction_log 
            SET retry_count = retry_count + 1,
                last_retry_at = NOW()
            WHERE transaction_id = %s
        """
        return self.db.execute_query(node, query, (transaction_id,))
    
    def get_pending_replications(self, source_node):
        """
        Get all pending replications from a source node.
        
        Args:
            source_node: Node to query for pending replications
            
        Returns:
            List of pending transaction records
        """
        query = """
            SELECT * FROM transaction_log
            WHERE source_node = %s
              AND status = 'PENDING'
              AND retry_count < max_retries
            ORDER BY created_at ASC
        """
        
        conn = self.db.get_connection(source_node)
        if not conn:
            logger.warning(f"Cannot query pending replications from {source_node} - node offline")
            return []
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, (source_node,))
            results = cursor.fetchall()
            
            if results:
                logger.info(f"Found {len(results)} pending replications on {source_node}")
            
            return results
        except Exception as e:
            logger.error(f"Error fetching pending replications from {source_node}: {e}")
            return []
        finally:
            conn.close()
    
    def get_failed_replications(self, source_node):
        """
        Get all permanently failed replications (exceeded max retries).
        
        Args:
            source_node: Node to query
            
        Returns:
            List of failed transaction records
        """
        query = """
            SELECT * FROM transaction_log
            WHERE source_node = %s
              AND (status = 'FAILED' OR (status = 'PENDING' AND retry_count >= max_retries))
            ORDER BY created_at DESC
            LIMIT 100
        """
        
        conn = self.db.get_connection(source_node)
        if not conn:
            return []
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, (source_node,))
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching failed replications from {source_node}: {e}")
            return []
        finally:
            conn.close()
    
    def _generate_transaction_id(self):
        """Generate unique transaction ID using UUID"""
        import uuid
        return str(uuid.uuid4())