const API_URL = ""; // change to http://ccscloud.dlsu.edu.ph:60157/

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
        <div class="results-content original-value-content">
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
                <h6 class="last-update">Last updated on ${formatDate(
                  data.last_updated
                )}</h6>
            </div>
        </div>

        <div class="bottom-info">
          <div class="detail"><b>Read Duration:</b> ${result.duration}s</div>
          <div class="detail"><b>Isolation Level:</b> ${result.isolation_level}</div>
          <div class="detail"><b>Repeatable:</b> ${result.repeatable}</div>
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
                <h6 class="last-update">Last updated on ${formatDate(
                  data.last_updated
                )}</h6>
            </div>
        </div>
    </div>
    `;
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
                    <div class="runtime-minutes">${
                      data.runtime_minutes
                    } minutes</div>
                    <div class="separator">|</div>
                    <div class="start-year">${data.start_year}</div>
                </div>
                <h6 class="last-update">Last updated on ${formatDate(
                  data.last_updated
                )}</h6>
            </div>
        </div>
    </div>
    `;
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
                        <div class="runtime-minutes">${
                          data.read1.runtime_minutes
                        } minutes</div>
                        <div class="separator">|</div>
                        <div class="start-year">${data.read1.start_year}</div>
                    </div>
                    <h6 class="last-update">Last updated on ${formatDate(
                      data.read1.last_updated
                    )}</h6>
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
                        <div class="runtime-minutes">${
                          data.read2.runtime_minutes
                        } minutes</div>
                        <div class="separator">|</div>
                        <div class="start-year">${data.read2.start_year}</div>
                    </div>
                    <h6 class="last-update">Last updated on ${formatDate(
                      data.read2.last_updated
                    )}</h6>
                </div>
            </div>
        </div>
        <div class="read-results minimal">
        <div class="row info-row">
          <div><b>Duration:</b> ${data.duration}s</div>
          <div><b>Blocked:</b> ${data.blocked}</div>
        </div>
        <div class="row info-row">
          <div><b>Original Runtime:</b> ${data.original_runtime}</div>
          <div><b>Saw New Value:</b> ${data.saw_new_value}</div>
        </div>
        <div class="row info-row">
          <div><b>Type:</b> ${nonRepeatableText}</div>
          <div><b>Values Changes Between Reads:</b> ${data.values_changed_between_reads}</div>
        </div>
        <div class="row info-row">
          <div></div>
          <div><b>Read During Write:</b> ${data.read_during_write}</div>
        </div>
        <div class="row status-row">
          <div class="read-status ${statusClass}">${
    data.success ? "Successful" : "Unsuccessful"
  } Read</div>
          <div class="timestamp">${formatDate(data.timestamp)}</div>
        </div>
      </div>
    </div>
    `;
}

/********************************************
 * HELPER — BUILD WRITER NODE RESULT CARD
 ********************************************/
function buildWriterNodeCard(nodeName, writerIndex, version, result) {
  if (!result.success) {
    return `
      <div class="node-result">
        <h3>${nodeName} (Writer ${writerIndex})</h3>
        <div class="writer-results minimal">
          <div class="status-row">
            <div class="write-status failed">Failed</div>
            <div class="timestamp">${result.error || "Unknown error"}</div>
          </div>
        </div>
      </div>
    `;
  }

  const data = result;
  const statusClass = data.success ? "success" : "failed";

  if (version === 2) {

    return `
        <div class="node-result node-writer" id="${nodeName}-writer">
          <h2>${nodeName} (Writer ${writerIndex})</h2>

          <div class="results-content write-1-content" id="${nodeName}-writer-content">
                <div class="top">
                    <div class="row info-row"><b>Genres:</b> ${data.data_written.genres}</div>
                    <div class="row info-row"><b>Runtime Minutes:</b> ${data.data_written.runtime_minutes} minutes</div>
                </div>

                </div>
                <div class="writer-results minimal">
                  <div class="row info-row">
                    <div><b>Duration:</b> ${data.duration}s</div>
                  </div>
                  <div class="row info-row">
                    <div><b>Rows Affected:</b> ${data.rows_affected}</div>
                    <div></div>
                  </div>
                  <div class="row status-row">
                    <div class="write-status ${statusClass}">${data.success ? "Successful Write" : "Unsuccessful Write"}</div>
                    <div class="timestamp">Written at ${formatDate(data.timestamp)}</div>
                  </div>
                </div>
    
        </div>
      `;

  } else {
      
      return `
        <div class="node-result node-writer" id="${nodeName}-writer">
          <h2>${nodeName} (Writer ${writerIndex})</h2>
    
          <div class="writer-results minimal">
            <!--<div class="row info-row">
              <div><b>Commit Time:</b> ${data.timestamp}s</div>
            </div>-->
            <div class="row info-row">
              <div><b>Duration:</b> ${data.duration}s</div>
            </div>
            <!--<div class="row info-row">
              <div><b>Rows Affected:</b> ${data.rows_affected}</div>
              <div></div>
            </div>-->
            <div class="row status-row">
              <div class="write-status ${statusClass}">${data.success ? "Successful Write" : "Unsuccessful Write"}</div>
              <div class="timestamp">Written at ${formatDate(data.timestamp)}</div>
            </div>
          </div>
        </div>
      `;

  }

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

    const response = await fetch(`${API_URL}/test/concurrent-read`, {
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

      let consistencyText = "";
      if (result.consistent) {
        consistencyText = "Final states are consistent across all nodes.";
      } else {
        consistencyText = "Final states are not consistent across all nodes.";
      }

      document.getElementById("case1-consistency-val").textContent = consistencyText;

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
  const runtime_minutes = document.getElementById(
    "case-2-runtime_minutes"
  ).value;
  const iso = document.getElementById("case-2-isolation-level").value;

  const body = {
    tconst,
    new_data: {
      runtime_minutes: runtime_minutes,
    },
    isolation_level: iso,
  };

  const jsonSection = document.getElementById("json-response-c2");
  const jsonBox = document.getElementById("json-content-c2");

  const originalValueContainer = document.querySelector(
    ".original-value-container"
  );
  const finalValuesContainer = document.querySelector(
    ".final-values-container"
  );
  const readersContainer = document.querySelector(".readers-container");
  const writersContainer = document.querySelector(".write-content-container");

  const nodeContainer = document.querySelector(".node-results-container");
  const analysisContainer = document.querySelector(".results-analysis");

  jsonSection.style.display = "none";
  nodeContainer.style.display = "none";
  analysisContainer.style.display = "none";

  try {
    console.log(body);

    const response = await fetch(
      `${API_URL}/test/read-write-conflict`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }
    );

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

    document.getElementById("case2-node-results-container").style.display = "flex";

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
      const cardHTML = buildFinalNodeCard(key, finalValues[key]);
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
        1,
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
      document.getElementById("case2-result-analysis").style.display = "flex";

      let consistencyText = "";
      if (result.analysis.final_state_consistent_across_nodes) {
        consistencyText = "Final states are consistent across all nodes.";
      } else {
        consistencyText = "Final states are not consistent across all nodes.";
      }

      document.getElementById("consistency-val").textContent = consistencyText;
      document.getElementById("explanation-val").textContent =
        result.analysis.explanation;

      document.getElementById("dirty-read-val").textContent =
        result.analysis.values_changed_between_reads;

      //document.getElementById("nonrepeat-val").textContent =
        //result.analysis.non_repeatable_reads;

      document.getElementById("blocking-val").textContent =
        result.analysis.blocking_occurred;

      document.getElementById("readers-count").textContent =
        result.analysis.readers_succeeded || "";

      document.getElementById("writers-count").textContent =
        result.analysis.writers_succeeded || "";

      document.getElementById("readers-duration").textContent = result.analysis
        .average_reader_duration
        ? `Average duration: ${result.analysis.average_reader_duration} seconds`
        : "";

      document.getElementById("writers-duration").textContent = result.analysis
        .average_writer_duration
        ? `Average duration: ${result.analysis.average_writer_duration} seconds`
        : "";

      analysisContainer.style.display = "flex";
    }
  } catch (err) {
    console.error(err);
    alert("Error contacting server.");
  }
});

/********************************************
 * CASE 3 — FORM SUBMIT HANDLER
 ********************************************/
document.getElementById("case-3-form").addEventListener("submit", async (e) => {
  e.preventDefault();

  const tconst = document.getElementById("case-3-tconst").value;
  const runtime_minutes_1 = document.getElementById(
    "case-3-runtime_minutes_1"
  ).value;
  const genres_1 = document.getElementById("case-3-genres_1").value;
  const runtime_minutes_2 = document.getElementById(
    "case-3-runtime_minutes_2"
  ).value;
  const genres_2 = document.getElementById("case-3-genres_2").value;
  const runtime_minutes_3 = document.getElementById(
    "case-3-runtime_minutes_3"
  ).value;
  const genres_3 = document.getElementById("case-3-genres_3").value;
  const iso = document.getElementById("case-3-isolation-level").value;

  const body = {
    tconst,
    updates: [
        {runtime_minutes: runtime_minutes_1,genres: genres_1},
        {runtime_minutes: runtime_minutes_2,genres: genres_2},
        {runtime_minutes: runtime_minutes_3,genres: genres_3},
    ],
    isolation_level: iso,
  };

  const jsonSection = document.getElementById("json-response-c3");
  const jsonBox = document.getElementById("json-content-c3");

  const finalValuesContainer = document.getElementById("case3-final-values-container");
  const writersContainer = document.getElementById("case3-writers-container");

  const nodeContainer = document.querySelector(".node-results-container");
  const analysisContainer = document.querySelector(".results-analysis");

  jsonSection.style.display = "none";
  nodeContainer.style.display = "none";
  analysisContainer.style.display = "none";

  try {
    const response = await fetch(
      `${API_URL}/test/concurrent-write`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }
    );

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

    // Final Values
    finalValuesContainer.innerHTML = "";

    const finalValues = result.results.final_values;
    document.getElementById("case3-node-results-container").style.display = "flex";

    let index = 1;
    for (let key in finalValues) {
      const cardHTML = buildFinalNodeCard(key, finalValues[key]);
      finalValuesContainer.innerHTML += cardHTML;
      index++;
    }

    if (index > 1) finalValuesContainer.style.display = "flex";

    writersContainer.innerHTML = "";

    const writers = result.results.writers;

    // writers
    index = 1;
    for (let key in writers) {
      const cardHTML = buildWriterNodeCard(
        key.split("_").pop(),
        index,
        2,
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

      document.getElementById("c3-explanation").style.display = "flex";

      let consistencyText = "";
      if (result.analysis.final_state_consistent_across_nodes) {
        consistencyText = "Final states are consistent across all nodes.";
      } else {
        consistencyText = "Final states are not consistent across all nodes.";
      }

      document.getElementById("consistency-val-c3").textContent = consistencyText;
      document.getElementById("explanation-val-c3").textContent =
        result.analysis.explanation;

      document.getElementById("blocking-val-c3").textContent =
        result.analysis.blocking_occurred;

      document.getElementById("deadlock-val").textContent =
        result.analysis.deadlocks_detected;

      //document.getElementById("serialization-val").textContent =
        //result.analysis.serialization_enforced;

      document.getElementById("failed-writes-count").textContent =
        result.analysis.failed_writes || "0";

      document.getElementById("successful-writes-count").textContent =
        result.analysis.successful_writes || "";

      document.getElementById("writers-duration").textContent = result.analysis
        .average_writer_duration
        ? `Average duration: ${result.analysis.average_writer_duration} seconds`
        : "";

      analysisContainer.style.display = "flex";
    }
  } catch (err) {
    console.error(err);
    alert("Error contacting server.");
  }
});
