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
 * HELPER — BUILD ORIGINAL NODE RESULT CARD
 ********************************************/
function buildOriginalNodeCard(result) {

  const data = result;

  return `
    <div class="node-result original-value-container">
        <div class="results-content original-value-content">
            <div class="top">
                <div class="title"><b>${data.primary_title}</b></div>
                <div class="genre">${data.genres}</div>
            </div>
            <div class="bottom">
                <div class="row">
                    <div class="runtime-minutes">${data.runtime_minutes}</div>
                    <div class="separator">|</div>
                    <div class="start-year">${data.start_year}</div>
                </div>
                <h6 class="last-update">Last updated on ${formatDate(data.last_updated)}</h6>
            </div>
        </div>
    </div>
    `
}

/********************************************
 * HELPER — BUILD FINAL NODE RESULT CARD
 ********************************************/
function buildFinalNodeCard(nodeName, result) {

  const data = result;

  return `
    <div class="node-result ${nodeName}-final-value">
        <h2>${nodeName}</h2>
        <div class="results-content final-value-content">
            <div class="top">
                <div class="title"><b>${data.primary_title}</b></div>
                <div class="genre">${data.genres}</div>
            </div>
            <div class="bottom">
                <div class="row">
                    <div class="runtime-minutes">${data.runtime_minutes} minutes</div>
                    <div class="separator">|</div>
                    <div class="start-year">${data.start_year}</div>
                </div>
                <h6 class="last-update">Last updated on ${formatDate(data.last_updated)}</h6>
            </div>
        </div>
    </div>
    `
}

/********************************************
 * HELPER — BUILD READER NODE RESULT CARD
 ********************************************/
function buildReaderNodeCard(nodeName, readerIndex, result) {
  if (!result.success) {
    return `
      <div class="node-result">
      <h3>${nodeName}</h3>
        <div class="read-results">
          <div class="read-status failed">Failed</div>
          <div class="timestamp">${result.error || "Unknown error"}</div>
        </div>
      </div>
    `;
  }

  const data = result;

  const statusClass = data.success ? "success" : "failed";

  let nonRepeatableText = "";

  if (data.non_repeatable_read) {
    nonRepeatableText = "Non Repeatable";
  } else {
    nonRepeatableText = "Repeatable";
  }

  return `
    <div class="node-result node-reader" id="${nodeName}-reader">
        <h2>${nodeName} (Reader ${readerIndex})</h2>
        <div class="read-content-container">
            <div class="results-content read-1-content">
                <div class="top">
                    <h3 class="read-1"><b>Read 1</b></h3>
                    <div class="title"><b>${data.read1.primary_title}</b></div>
                    <div class="genre">${data.read1.genres}</div>
                </div>
                <div class="bottom">
                    <div class="row">
                        <div class="runtime-minutes">${data.read1.runtime_minutes} minutes</div>
                        <div class="separator">|</div>
                        <div class="start-year">${data.read1.start_year}</div>
                    </div>
                    <h6 class="last-update">Last updated on ${formatDate(data.read1.last_updated)}</h6>
                </div>
            </div>
            <div class="results-content read-2-content">
                <div class="top">
                    <h3 class="read-1"><b>Read 2</b></h3>
                    <div class="title"><b>${data.read2.primary_title}</b></div>
                    <div class="genre">${data.read2.genres}</div>
                </div>
                <div class="bottom">
                    <div class="row">
                        <div class="runtime-minutes">${data.read2.runtime_minutes} minutes</div>
                        <div class="separator">|</div>
                        <div class="start-year">${data.read2.start_year}</div>
                    </div>
                    <h6 class="last-update">Last updated on ${formatDate(data.read2.last_updated)}</h6>
                </div>
            </div>
        </div>
        <div class="read-results">
            <div class="top">
                <div class="left">
                    <div class="duration"><b>Duration:</b> ${data.duration} seconds</div>
                    <div class="blocked"><b>Blocked:</b> ${data.blocked}</div>
                    <div class="dirty-read-detected"><b>Dirty Read Detected:</b> ${data.dirty_read_detected}</div>
                </div>
                <div class="right">
                    <div class="original-data"><b>Original Runtime:</b> ${data.original_runtime}</div>
                    <div class="read-during-write"><b>Read During Write:</b> ${data.read_during_write}</div>
                    <div class="saw-uncommitted-write"><b>Saw Uncommitted Write:</b> ${data.saw_uncommitted_write}</div>
                </div>
            </div>
            <div class="bottom">
                <h5 class="repeatable">${nonRepeatableText}</h5>
                <div class="end">
                    <div class="read-status ${statusClass}">${data.success ? "Successful Read" : "Unsuccessful Read"}</div>
                    <h6 class="timestamp">Read at ${formatDate(data.timestamp)}</h6>
                </div>
            </div>
        </div>
    </div>
    `
}

/********************************************
 * HELPER — BUILD WRITER NODE RESULT CARD
 ********************************************/
function buildWriterNodeCard(nodeName, writerIndex, result) {
  if (!result.success) {
    return `
      <div class="node-result">
      <h3>${nodeName} (${writerIndex})</h3>
        <div class="read-results">
          <div class="read-status failed">Failed</div>
          <div class="timestamp">${result.error || "Unknown error"}</div>
        </div>
      </div>
    `;
  }

  const data = result;
  const statusClass = data.success ? "success" : "failed";
  return `
    <div class="node-result node-writer" id="${nodeName}-writer">
            
        <h2>${nodeName} (Writer ${writerIndex})</h2>

        <div class="read-results">
            <div class="top">
                <div class="left">
                    <div class="commit-time"><b>Commit Time:</b> ${data.commit_time} seconds</div>
                    <div class="duration"><b>Duration:</b> ${data.commit_time} seconds</div>
                </div>
            </div>
            <div class="bottom">
                <h5 class="rows-affected"><b>${data.rows_affected} rows affected</b></h5>
                <div class="end">
                    <div class="read-status ${statusClass}">${data.success ? "Successful Write" : "Unsuccessful Write"}</div>
                    <h6 class="timestamp">written at ${formatDate(data.timestamp)}</h6>
                </div>
            </div>
        </div>
    </div>
    `
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

/********************************************
 * CASE 2 — FORM SUBMIT HANDLER
 ********************************************/
document.getElementById("case-2-form").addEventListener("submit", async (e) => {
  e.preventDefault();

  const tconst = document.getElementById("case-2-tconst").value;
  const title_type = document.getElementById("case-2-title_type").value;
  const primary_type = document.getElementById("case-2-primary_title").value;
  const start_year = document.getElementById("case-2-start_year").value;
  const runtime_minutes = document.getElementById("case-2-runtime_minutes").value;
  const genres = document.getElementById("case-2-genres").value;
  const iso = document.getElementById("isolation-level").value;

  const body = { tconst, 
                new_data: {
                    runtime_minutes: runtime_minutes,
                },
                isolation_level: iso };

  const jsonSection = document.querySelector(".json-response");
  const jsonBox = document.querySelector(".json-content");

  const originalValueContainer = document.querySelector(".original-value-container");
  const finalValuesContainer = document.querySelector(".final-values-container");
  const readersContainer = document.querySelector(".readers-container");
  const writersContainer = document.querySelector(".write-content-container");

  const nodeContainer = document.querySelector(".node-results-container");
  const analysisContainer = document.querySelector(".results-analysis");

  jsonSection.style.display = "none";
  nodeContainer.style.display = "none";
  analysisContainer.style.display = "none";

  try {
    console.log(body);

    const response = await fetch("http://localhost:80/test/read-write-conflict", {
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

    // Original Value
    originalValueContainer.innerHTML = ""; 
    originalCardHTML = buildOriginalNodeCard(result.results.original_value);
    originalValueContainer.innerHTML += originalCardHTML;
    originalValueContainer.style.display = "flex"; 

    // Final Values
    finalValuesContainer.innerHTML = ""; 

    const finalValues = result.results.final_values;

    let index = 1;
    for (let key in finalValues) {
      const cardHTML = buildFinalNodeCard(
        key,
        finalValues[key]
      );
      finalValuesContainer.innerHTML += cardHTML;
      index++;
    }

    if (index > 1) finalValuesContainer.style.display = "flex";

    readersContainer.innerHTML = "";

    const readers = result.results.readers;

    // readers
    index = 1;
    for (let key in readers) {
      const cardHTML = buildReaderNodeCard(
        key.split("_")[0],
        index,
        readers[key]
      );
      readersContainer.innerHTML += cardHTML;
      index++;
    }

    if (index > 1) readersContainer.style.display = "flex";

    writersContainer.innerHTML = "";

    const writers = result.results.writers;

    // readers
    index = 1;
    for (let key in writers) {
      const cardHTML = buildWriterNodeCard(
        key.split("_")[0],
        index,
        writers[key]
      );
      writersContainer.innerHTML += cardHTML;
      index++;
    }

    if (index > 1) writersContainer.style.display = "flex";


    /**********************************
     * C. ANALYSIS SECTION
     **********************************/
    if (result.analysis) {
      const consistencyEl = analysisContainer.querySelector(".consistency");
      const explanationEl = analysisContainer.querySelector(".explanation");
      const dirtyReadEl = analysisContainer.querySelector(".dirty-read-anomaly");
      const nonRepeatableEl = analysisContainer.querySelector(".non-repeatable-read-anomaly");
      const blockingEl = analysisContainer.querySelector(".blocking-anomaly");
      const readStatusEl = analysisContainer.querySelector(".readers-succeeded");
      const writeStatusEl = analysisContainer.querySelector(".writers-succeeded");
      const avgDurReadersEl = analysisContainer.querySelector(".avg-duration-readers");
      const avgDurWritersEl = analysisContainer.querySelector(".avg-duration-writers");

      let consistencyText = "";
      if (result.analysis.final_state_consistent_across_nodes) {
        consistencyText = "Final states are consistent across all nodes.";
      } else {
        consistencyText = "Final states are not consistent across all nodes.";
      }

      let readSuccessfulText = "";
      if (result.analysis.readers_succeeded) {
        readSuccessfulText = `${result.analysis.readers_succeeded} Successful Readers`;
      } else {
        readSuccessfulText = "No Successful Readers";
      }

      let writeSuccessfulText = "";
      if (result.analysis.writers_succeeded) {
        writeSuccessfulText = `${result.analysis.writers_succeeded} Successful Writers`;
      } else {
        writeSuccessfulText = "No Successful Writers";
      }

      if (explanationEl)
        explanationEl.textContent = result.analysis.explanation || "";

      if (consistencyEl)
        consistencyEl.textContent = consistencyText || "";

      if (dirtyReadEl)
        dirtyReadEl.textContent = result.analysis.dirty_reads_occurred || "";

      if (nonRepeatableEl)
        nonRepeatableEl.textContent = result.analysis.non_repeatable_reads || "";

      if (blockingEl)
        blockingEl.textContent = result.analysis.blocking_occurred || "";

      if (readStatusEl)
        readStatusEl.innerHTML = `<b>${readSuccessfulText}</b>` || "";

      if (writeStatusEl)
        writeStatusEl.innerHTML = `<b>${writeSuccessfulText}</b>` || "";

      if (avgDurReadersEl)
        
        avgDurReadersEl.textContent = `with an average duration of ${result.analysis.average_reader_duration} seconds` || "";

      if (avgDurWritersEl)
        avgDurWritersEl.textContent = `with an average duration of ${result.analysis.average_writer_duration} seconds` || "";


      analysisContainer.style.display = "flex";
    }

  } catch (err) {
    console.error(err);
    alert("Error contacting server.");
  }
});
