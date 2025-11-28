const API_URL = "http://localhost:80";

document.addEventListener("DOMContentLoaded", () => {
  let currentPage = 1;
  const limit = 20;
  let currentQuery = {
    q: "",
    year_from: "",
    year_to: "",
    genre: "",
    type: "",
  };

  function renderTitles(list) {
    const container = document.getElementById("titlesList");
    container.innerHTML = "";

    if (!list || list.length === 0) {
      container.innerHTML = `<p>No titles found.</p>`;
      return;
    }

    list.forEach((movie) => {
      container.innerHTML += `
        <div class="title-item">
          <div class="title-left">
            <span><strong>TCONST:</strong> ${movie.tconst}</span>
            <span class="title-name">${movie.primary_title}</span>
          </div>
          <div class="title-right">
            <span><strong>Year:</strong> ${movie.start_year}</span>
            <span><strong>Minutes:</strong> ${movie.runtime_minutes}</span>
            <span><strong>Genre:</strong> ${movie.genres}</span>
            <span><strong>Type:</strong> ${movie.title_type}</span>
          </div>
        </div>
      `;
    });

    document.getElementById("pageNumber").innerText = `Page ${currentPage}`;
  }

  async function getFilteredMovies() {
    try {
      let url;
      const params = new URLSearchParams({
        page: currentPage,
        limit: limit,
      });

      // If searching, add search parameters and call /titles/search
      if (currentQuery.q || currentQuery.year_from || currentQuery.year_to || currentQuery.type || currentQuery.genre) {
        if (currentQuery.q) params.append("q", currentQuery.q);
        if (currentQuery.year_from) params.append("year_from", currentQuery.year_from);
        if (currentQuery.year_to) params.append("year_to", currentQuery.year_to);
        if (currentQuery.type) params.append("type", currentQuery.type);
        if (currentQuery.genre) params.append("genre", currentQuery.genre);

        url = `${API_URL}/titles/search?${params.toString()}`;
      } else {
        // No search, call /titles
        if (currentQuery.type) params.append("type", currentQuery.type);
        url = `${API_URL}/titles?${params.toString()}`;
      }

      console.log("Fetching URL:", url);

      const res = await fetch(url);
      if (!res.ok) throw new Error(await res.text());

      const data = await res.json();
      console.log(data.data)

      return data.data
    } catch (err) {
      console.error("Error fetching titles:", err);
      return [];
    }
  }

  async function showPage() {
    const list = await getFilteredMovies();
    renderTitles(list);
  }

  document.getElementById("searchBtn").addEventListener("click", async () => {
    currentQuery.q = document.getElementById("searchInput").value.trim();
    currentQuery.year_from = document.getElementById("yearFrom").value.trim();
    currentQuery.year_to = document.getElementById("yearTo").value.trim();
    currentQuery.type = document.getElementById("typeSelect").value;
    currentQuery.genre = document.getElementById("genreSelect").value;

    currentPage = 1;
    await showPage();
  });

  document.getElementById("nextBtn").addEventListener("click", async () => {
    currentPage++;
    await showPage();
  });

  document.getElementById("prevBtn").addEventListener("click", async () => {
    if (currentPage > 1) currentPage--;
    await showPage();
  });

  showPage();
});
