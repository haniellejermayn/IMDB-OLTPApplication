const API_URL = "http://localhost:80";

const form = document.getElementById("title-form");
const resultDiv = document.getElementById("result");

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

    try {
        console.log("Form Data: ", formData);
        const res = await fetch(`${API_URL}/title`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData)
        });

        if (res.ok) {
            resultDiv.textContent = `✅ Title ${formData.tconst} created/updated successfully!`;
            resultDiv.style.color = "#452829";
            form.reset();
        } else {
            const err = await res.text();
            resultDiv.textContent = `❌ Error: ${err}`;
            resultDiv.style.color = "#E83D3D";
        }
    } catch (error) {
        console.error(error);
        resultDiv.textContent = `❌ Error: ${error.message}`;
        resultDiv.style.color = "#E83D3D";
    }
});