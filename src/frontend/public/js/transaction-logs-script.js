const dummyLogs = [
  {
    log_id: 1,
    transaction_id: "TX123",
    source_node: "node1",
    target_node: "node2",
    operation_type: "INSERT",
    table_name: "title",
    record_id: "tt0000001",
    status: "SUCCESS",
    retry_count: 0,
    max_retries: 3,
    error_message: null,
    created_at: "2025-11-28 08:15:00",
    last_retry_at: null,
    completed_at: "2025-11-28 08:15:05",
  },
  {
    log_id: 2,
    transaction_id: "TX124",
    source_node: "node2",
    target_node: "node3",
    operation_type: "UPDATE",
    table_name: "title",
    record_id: "tt0000002",
    status: "FAILED",
    retry_count: 1,
    max_retries: 3,
    error_message: "Duplicate entry",
    created_at: "2025-11-28 08:20:00",
    last_retry_at: "2025-11-28 08:25:00",
    completed_at: null,
  },
  {
    log_id: 3,
    transaction_id: "TX125",
    source_node: "node3",
    target_node: "node1",
    operation_type: "DELETE",
    table_name: "title",
    record_id: "tt0000003",
    status: "PENDING",
    retry_count: 0,
    max_retries: 3,
    error_message: null,
    created_at: "2025-11-28 08:30:00",
    last_retry_at: null,
    completed_at: null,
  },
];

function renderLogs(logs) {
  const tableBody = document.querySelector("#logsTable tbody");
  tableBody.innerHTML = "";

  if (!logs.length) {
    tableBody.innerHTML = '<tr><td colspan="14">No logs found.</td></tr>';
    return;
  }

  logs.forEach((log) => {
    const row = document.createElement("tr");
    const statusClass =
      log.status === "SUCCESS"
        ? "text-main"
        : log.status === "FAILED"
        ? "text-secondary"
        : "text-sub";
    row.innerHTML = `
                <td>${log.log_id}</td>
                <td>${log.transaction_id}</td>
                <td>${log.source_node}</td>
                <td>${log.target_node}</td>
                <td>${log.operation_type}</td>
                <td>${log.table_name}</td>
                <td>${log.record_id}</td>
                <td class="${statusClass}">${log.status}</td>
                <td>${log.retry_count}</td>
                <td>${log.max_retries}</td>
                <td>${log.error_message || "-"}</td>
                <td>${log.created_at}</td>
                <td>${log.last_retry_at || "-"}</td>
                <td>${log.completed_at || "-"}</td>
            `;
    tableBody.appendChild(row);
  });
}

document
  .getElementById("refreshBtn")
  .addEventListener("click", () => renderLogs(dummyLogs));

// Initial render with dummy data
renderLogs(dummyLogs);

/*
async function fetchLogs() {
    const limit = document.getElementById('limit').value;
    const tableBody = document.querySelector('#logsTable tbody');
    tableBody.innerHTML = '<tr><td colspan="14">Loading...</td></tr>';

    try {
        const response = await fetch(`/logs?limit=${limit}`);
        const data = await response.json();
        renderLogs(data);
    } catch (err) {
        tableBody.innerHTML = `<tr><td colspan="14">Error fetching logs: ${err}</td></tr>`;
    }
}
document.getElementById('refreshBtn').addEventListener('click', fetchLogs);
fetchLogs();
*/
