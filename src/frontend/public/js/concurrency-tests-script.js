/********************************************
 * CASE SWITCHING
 ********************************************/
function showCase(caseNumber) {
  ["case-1", "case-2", "case-3"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.style.display = id === `case-${caseNumber}` ? "block" : "none";
  });

  ["case-1-option", "case-2-option", "case-3-option"].forEach((btn, idx) => {
    const el = document.getElementById(btn);
    if (el) {
      if (idx + 1 === caseNumber) el.classList.add("active-case-btn");
      else el.classList.remove("active-case-btn");
    }
  });
}

document.getElementById("case-1-option").onclick = () => showCase(1);
document.getElementById("case-2-option").onclick = () => showCase(2);
document.getElementById("case-3-option").onclick = () => showCase(3);

showCase(1);

/********************************************
 * JSON PRETTY FORMATTER (console-style)
 ********************************************/
function syntaxHighlight(json) {
  if (typeof json !== "string") {
    json = JSON.stringify(json, null, 4);
  }

  json = json
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  return json.replace(
    /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(?:\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d+)?)/g,
    (match) => {
      let cls = "number";
      if (/^"/.test(match)) {
        cls = /:$/.test(match) ? "key" : "string";
      } else if (/true|false/.test(match)) {
        cls = "boolean";
      } else if (/null/.test(match)) {
        cls = "null";
      }
      return `<span class="${cls}">${match}</span>`;
    }
  );
}

/********************************************
 * HELPER — BUILD NODE RESULT CARD
 ********************************************/
function buildNodeCard(nodeName, readerIndex, result) {
  if (!result.success) {
    return `
      <div class="node-result">
        <h3>${nodeName} - Reader ${readerIndex}</h3>
        <div class="read-results">
          <div class="read-status failed">Failed</div>
          <div class="timestamp">${result.error || "Unknown error"}</div>
        </div>
      </div>
    `;
  }

  const data = result.data;

  return `
    <div class="node-result">
      <h3 class="node-name">${nodeName} - Reader ${readerIndex}</h3>
      <div class="read-results">
        <div class="results-content">
          <!-- Movie details with indent -->
          <div class="detail title">${data.primary_title} (${
    data.title_type
  })</div>
          <div class="detail genre">Genre: ${data.genres}</div>
          <div class="detail">Runtime: ${data.runtime_minutes} min</div>
          <div class="detail">Year: ${data.start_year}</div>
          <div class="detail">TCONST: ${data.tconst}</div>
          <div class="detail">Last Updated: ${formatDate(data.last_updated)}</div>
        </div>

        <div class="bottom-info">
          <div class="detail">Read Duration: ${result.duration}s</div>
          <div class="detail">Isolation Level: ${result.isolation_level}</div>
          <div class="read-status successful">Successful Read</div>
          <div class="timestamp">${formatDate(result.timestamp)}</div>
        </div>
      </div>
    </div>
  `;
}

/********************************************
 * HELPER — DATE FORMATTER
 ********************************************/
function formatDate(dateStr) {
  if (!dateStr) return "N/A";
  const date = new Date(dateStr);
  return new Intl.DateTimeFormat("en-US", {
    weekday: "short",
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

/********************************************
 * CASE 1 — FORM SUBMIT HANDLER
 ********************************************/
document.getElementById("case-1-form").addEventListener("submit", async (e) => {
  e.preventDefault();

  const tconst = document.getElementById("tconst").value;
  const iso = document.getElementById("isolation-level").value;

  const body = { tconst, isolation_level: iso };

  const jsonSection = document.querySelector(".json-response");
  const jsonBox = document.querySelector(".json-content");

  const nodeContainer = document.querySelector(".node-results-container");
  const analysisContainer = document.querySelector(".results-analysis");

  jsonSection.style.display = "none";
  nodeContainer.style.display = "none";
  analysisContainer.style.display = "none";

  try {
    console.log(body);

    const response = await fetch("http://localhost:80/test/concurrent-read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      alert("Request failed");
      return;
    }

    const result = await response.json();
    console.log("Server Response:", result);

    /**********************************
     * A. JSON RESPONSE (formatted)
     **********************************/
    jsonBox.innerHTML = `<pre class="code-block" style="white-space: pre-wrap;">${syntaxHighlight(
      result
    )}</pre>`;
    if (result && Object.keys(result).length)
      jsonSection.style.display = "block";

    /**********************************
     * B. NODE RESULT CARDS
     **********************************/
    nodeContainer.innerHTML = ""; // clear old results

    let index = 1;
    for (let key in result.results) {
      const cardHTML = buildNodeCard(
        key.split("_")[0],
        index,
        result.results[key]
      );
      nodeContainer.innerHTML += cardHTML;
      index++;
    }

    if (index > 1) nodeContainer.style.display = "flex";

    /**********************************
     * C. ANALYSIS SECTION
     **********************************/
    if (result.analysis) {
      const explanationEl = analysisContainer.querySelector(".explanation");

      if (explanationEl)
        explanationEl.textContent = result.analysis.explanation || "";

      analysisContainer.style.display = "block";
    }
  } catch (err) {
    console.error(err);
    alert("Error contacting server.");
  }
});
