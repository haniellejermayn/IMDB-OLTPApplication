const API_URL = "http://localhost:80";

document.addEventListener("DOMContentLoaded", async () => {
  const form = document.getElementById("title-form");
  const resultDiv = document.getElementById("result");

  // Detect edit mode ------------------------------------------------------
  const urlParts = window.location.pathname.split("/");
  const editTconst = urlParts.length > 2 ? urlParts[2] : null;
  const deleteBtn = document.getElementById("delete-button");

  if (editTconst) {
    document.getElementById("form-title").innerText = `EDITING: ${editTconst}`;
    await loadExistingTitle(editTconst);
  } else {
    if (deleteBtn) {
      deleteBtn.style.display = "none";
    }
  }

  const backLink = document.getElementById("back");
  if (editTconst) {
    backLink.text = "Back to Browse"
    backLink.href = "/browse"; // Go back to Browse if editing
  } else {
    backLink.href = "/"; // Default back to Dashboard
  }

  // Load title data when editing -------------------------------------------
  async function loadExistingTitle(tconst) {
    try {
      const res = await fetch(`${API_URL}/title/${tconst}`);
      if (!res.ok) throw new Error(await res.text());

      const data = await res.json();

      // Fill form
      document.getElementById("tconst").value = data.tconst;
      document.getElementById("title_type").value = data.title_type;
      document.getElementById("primary_title").value = data.primary_title;
      document.getElementById("start_year").value = data.start_year;
      document.getElementById("runtime_minutes").value = data.runtime_minutes;
      document.getElementById("genres").value = data.genres;

      // Lock TCONST in edit mode
      document.getElementById("tconst").setAttribute("readonly", true);
    } catch (error) {
      console.error(error);
      resultDiv.textContent = `❌ Failed to load title: ${error.message}`;
      resultDiv.style.color = "#E83D3D";
    }
  }

  // Form submit (create OR update) -----------------------------------------
  form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const formData = {
      tconst: document.getElementById("tconst").value.trim(),
      title_type: document.getElementById("title_type").value.trim(),
      primary_title: document.getElementById("primary_title").value.trim(),
      start_year: parseInt(document.getElementById("start_year").value),
      runtime_minutes: parseInt(
        document.getElementById("runtime_minutes").value
      ),
      genres: document.getElementById("genres").value.trim(),
    };

    console.log("Form data: ", formData);

    try {
      let uri;
      let method;

      if (editTconst) {
        uri = `${API_URL}/title/${editTconst}`;
        method = "PUT";
      } else {
        uri = `${API_URL}/title`;
        method = "POST";
      }

      const res = await fetch(uri, {
        method: method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData),
      });

      if (res.ok) {
        resultDiv.textContent = `✅ Title ${formData.tconst} saved successfully!`;
        resultDiv.style.color = "#452829";
      } else {
        resultDiv.textContent = `❌ Error: ${await res.text()}`;
        resultDiv.style.color = "#E83D3D";
      }
    } catch (error) {
      resultDiv.textContent = `❌ Error: ${error.message}`;
      resultDiv.style.color = "#E83D3D";
    }
  });

  deleteBtn.addEventListener("click", async () => {
    if (!editTconst) return;

    if (!confirm(`Are you sure you want to delete ${editTconst}?`)) return;

    try {
      const res = await fetch(`${API_URL}/title/${editTconst}`, {
        method: "DELETE",
      });

      if (res.ok) {
        alert(`Title ${editTconst} deleted successfully!`);
        window.location.href = "/browse";
      } else {
        alert(`Error deleting: ${await res.text()}`);
      }
    } catch (err) {
      alert(`Error: ${err.message}`);
    }
  });
});
