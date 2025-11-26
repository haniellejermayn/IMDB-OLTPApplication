from flask import Flask, request, jsonify
from flask_cors import CORS
from db_manager import DatabaseManager
from replication.replication_manager import ReplicationManager
import logging

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_manager = DatabaseManager()
replication_manager = ReplicationManager(db_manager)

replication_manager.recovery_handler.start_automatic_retry()
logger.info("Application started with automatic replication retry enabled")

@app.route('/health', methods=['GET'])
def health_check():
    """Check node health status"""
    return jsonify(db_manager.check_all_nodes())

@app.route('/titles', methods=['GET'])
def get_titles():
    """Get titles with pagination"""
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 20))
    title_type = request.args.get('type', None)
    
    result = db_manager.get_titles(page, limit, title_type)
    return jsonify(result)

@app.route('/titles/search', methods=['GET'])
def search_titles():
    """
    Search titles with filters
    Query params:
    - q: search term (searches in primary_title)
    - year_from: minimum year
    - year_to: maximum year
    - type: title_type (movie, short, tvSeries, etc.)
    - genre: genre filter
    - page: page number (default 1)
    - limit: results per page (default 20)
    
    Example: GET /titles/search?q=matrix&year_from=1990&type=movie
    """
    search_term = request.args.get('q', None)
    year_from = request.args.get('year_from', None, type=int)
    year_to = request.args.get('year_to', None, type=int)
    title_type = request.args.get('type', None)
    genre = request.args.get('genre', None)
    page = int(request.args.get('page', 1))
    limit = int(request.args.get('limit', 20))
    
    result = db_manager.search_titles(
        search_term=search_term,
        year_from=year_from,
        year_to=year_to,
        title_type=title_type,
        genre=genre,
        page=page,
        limit=limit
    )
    return jsonify(result)

@app.route('/title/<tconst>', methods=['GET'])
def get_title(tconst):
    """Get single title by ID"""
    result = db_manager.get_title_by_id(tconst)
    return jsonify(result)

@app.route('/title', methods=['POST'])
def create_title():
    """Create new title"""
    data = request.json
    result = replication_manager.insert_title(data)
    return jsonify(result), 201 if result['success'] else 500

@app.route('/title/<tconst>', methods=['PUT'])
def update_title(tconst):
    """Update existing title"""
    data = request.json
    isolation_level = request.args.get('isolation', 'READ COMMITTED')
    result = replication_manager.update_title(tconst, data, isolation_level)
    return jsonify(result)

@app.route('/title/<tconst>', methods=['DELETE'])
def delete_title(tconst):
    """Delete title"""
    result = replication_manager.delete_title(tconst)
    return jsonify(result)

# ==================== CONCURRENCY TEST CASES ====================

@app.route('/test/concurrent-read', methods=['POST'])
def test_concurrent_read():
    """
    Test Case 1: Concurrent reads
    Example: POST /test/concurrent-read
    Body: {"tconst": "tt0133093", "isolation_level": "READ COMMITTED"}
    """
    data = request.json
    tconst = data.get('tconst')
    isolation_level = data.get('isolation_level', 'READ COMMITTED')
    
    result = replication_manager.test_concurrent_reads(tconst, isolation_level)
    return jsonify(result)

@app.route('/test/read-write-conflict', methods=['POST'])
def test_read_write_conflict():
    """
    Test Case 2: One transaction writing while others read
    Example: POST /test/read-write-conflict
    Body: {
        "tconst": "tt0133093",
        "new_data": {"runtime_minutes": 150},
        "isolation_level": "READ COMMITTED"
    }
    """
    data = request.json
    tconst = data.get('tconst')
    new_data = data.get('new_data', {})
    isolation_level = data.get('isolation_level', 'READ COMMITTED')
    
    result = replication_manager.test_read_write_conflict(tconst, new_data, isolation_level)
    return jsonify(result)

@app.route('/test/concurrent-write', methods=['POST'])
def test_concurrent_write():
    """
    Test Case 3: Concurrent writes
    Example: POST /test/concurrent-write
    Body: {
        "updates": [
            {"tconst": "tt0001", "data": {"runtime_minutes": 120}},
            {"tconst": "tt0002", "data": {"runtime_minutes": 90}}
        ],
        "isolation_level": "SERIALIZABLE"
    }
    """
    data = request.json
    updates = data.get('updates', [])
    isolation_level = data.get('isolation_level', 'READ COMMITTED')
    
    result = replication_manager.test_concurrent_writes(updates, isolation_level)
    return jsonify(result)

# ==================== FAILURE RECOVERY TEST CASES ====================

@app.route('/test/failure/fragment-to-central', methods=['POST'])
def test_failure_case1():
    """
    Test Case #1: Fragment write succeeds, but central replication fails
    Instructions returned on how to simulate this
    """
    result = replication_manager.simulate_failure('fragment_to_central')
    return jsonify(result)

@app.route('/test/failure/central-recovery', methods=['POST'])
def test_failure_case2():
    """
    Test Case #2: Central node recovers and processes missed transactions
    """
    result = replication_manager.recover_node('node1')
    return jsonify(result)

@app.route('/test/failure/central-to-fragment', methods=['POST'])
def test_failure_case3():
    """
    Test Case #3: Central write succeeds (fallback), but fragment replication fails
    Instructions returned on how to simulate this
    """
    result = replication_manager.simulate_failure('central_to_fragment')
    return jsonify(result)

@app.route('/test/failure/fragment-recovery', methods=['POST'])
def test_failure_case4():
    """
    Test Case #4: Fragment node recovers and processes missed transactions
    Body: {"node": "node2"} or {"node": "node3"}
    """
    node = request.json.get('node', 'node2')
    result = replication_manager.recover_node(node)
    return jsonify(result)

@app.route('/recovery/status', methods=['GET'])
def recovery_status():
    """Get pending replication count"""
    result = replication_manager.get_pending_replications()
    return jsonify(result)

@app.route('/recovery/auto-retry', methods=['POST'])
def control_auto_retry():
    """
    Start or stop automatic retry
    POST /recovery/auto-retry
    Body: {"action": "start"} or {"action": "stop"}
    """
    action = request.json.get('action', 'start')
    
    if action == 'start':
        replication_manager.recovery_handler.start_automatic_retry()
        return jsonify({'message': 'Automatic retry started'})
    elif action == 'stop':
        replication_manager.recovery_handler.stop_automatic_retry()
        return jsonify({'message': 'Automatic retry stopped'})
    else:
        return jsonify({'error': 'Invalid action. Use "start" or "stop"'}), 400

@app.route('/logs', methods=['GET'])
def get_transaction_logs():
    """Get transaction logs"""
    limit = int(request.args.get('limit', 50))
    result = db_manager.get_transaction_logs(limit)
    return jsonify(result)

# ==================== ISOLATION LEVEL TESTING ====================

@app.route('/test/isolation-levels', methods=['POST'])
def test_isolation_levels():
    """
    Test all isolation levels with concurrent operations
    Body: {
        "tconst": "tt0001",
        "operation": "read" or "write" or "read_write",
        "new_data": {"runtime_minutes": 999}  // for write/read_write operations
    }
    """
    data = request.json
    tconst = data.get('tconst')
    operation = data.get('operation', 'read')
    new_data = data.get('new_data', {'runtime_minutes': 999})
    
    isolation_levels = [
        'READ UNCOMMITTED',
        'READ COMMITTED',
        'REPEATABLE READ',
        'SERIALIZABLE'
    ]
    
    results = {}
    
    for level in isolation_levels:
        if operation == 'read':
            result = replication_manager.test_concurrent_reads(tconst, level)
        elif operation == 'read_write':
            result = replication_manager.test_read_write_conflict(tconst, new_data, level)
        else:
            # For write test, create sample updates
            updates = [
                {'tconst': tconst, 'data': {'runtime_minutes': 100 + i}}
                for i in range(3)
            ]
            result = replication_manager.test_concurrent_writes(updates, level)
        
        results[level] = result
    
    return jsonify({
        'test': 'isolation_levels_comparison',
        'operation': operation,
        'tconst': tconst,
        'results': results
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)