const API_URL = "";

document.addEventListener("DOMContentLoaded", () => {

    loadPendingReplications();
    loadNodeStatus();

    async function loadPendingReplications() {
        try {
            const res = await fetch(`${API_URL}/recovery/status`);
            const data = await res.json();
            console.log("API response:", data);
    
            // Update pending replications
            updatePendingReplications("node-1", data.by_node.node1.pending_count ?? 0);
            updatePendingReplications("node-2", data.by_node.node2.pending_count ?? 0);
            updatePendingReplications("node-3", data.by_node.node3.pending_count ?? 0);
    
        } catch (error) {
            console.error("Error fetching pending replications:", error);
        }
    }
    
    async function loadNodeStatus() {
        try {
            const res = await fetch(`${API_URL}/health`);
            const data = await res.json();
            console.log("API response:", data);
    
            // Update node statuses
            updateNodeStatus("node-1", capitalize(data.node1.status));
            updateNodeStatus("node-2", capitalize(data.node2.status));
            updateNodeStatus("node-3", capitalize(data.node3.status));

            updateTitleCount("node-1", data.node1.record_count);
            updateTitleCount("node-2", data.node2.record_count);
            updateTitleCount("node-3", data.node3.record_count)
    
        } catch (error) {
            console.error("Error fetching node status:", error);
        }
    }
    
    function updatePendingReplications(nodeId, count) {
        const pendingRepElem = document.querySelector(`#${nodeId} .pending-rep`);
        pendingRepElem.textContent = `${count} Pending Replications`;
    }
    
    function updateNodeStatus(nodeId, status) {
        const statusElems = document.querySelector(`#${nodeId} .node-status`);
        statusElems.textContent = status;
    
        // Update health indicator color
        const indicator = document.querySelector(`#${nodeId} .node-health-indicator`);
        if (status === "Online") {
            indicator.style.backgroundColor = "#4CAF50";
        } else {
            indicator.style.backgroundColor = "#E83D3D";
        } 
    }

    function updateTitleCount(nodeId, count) {
        const statusElems = document.querySelector(`#${nodeId} .title-count`);
        statusElems.textContent = count;
    }

    function capitalize(s) {
        return s.charAt(0).toUpperCase() + s.slice(1);
    }
});