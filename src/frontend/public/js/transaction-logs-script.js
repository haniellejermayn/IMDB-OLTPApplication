const API_URL = "http://localhost:80";

function renderLogs(logs) {
  const tableBody = document.querySelector("#logsTable tbody");
  tableBody.innerHTML = "";

  if (!logs || logs.length === 0) {
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

async function fetchLogs() {
  const limit = document.getElementById("limit").value || 50;
  const tableBody = document.querySelector("#logsTable tbody");

  tableBody.innerHTML = '<tr><td colspan="14">Loading...</td></tr>';

  try {
    const res = await fetch(`${API_URL}/logs?limit=${limit}`);

    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    const data = await res.json();
    const logs = data.logs

    renderLogs(logs);
  } catch (err) {
    tableBody.innerHTML = `
      <tr><td colspan="14">Error fetching logs: ${err}</td></tr>
    `;
  }
}

fetchLogs();
document.getElementById("refreshBtn").addEventListener("click", fetchLogs);