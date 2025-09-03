// Authenticated fetch with CSRF
function authFetch(url, options = {}) {
  options.headers = Object.assign({}, options.headers || {}, {
    "X-CSRFToken": CSRF_TOKEN,
    "Content-Type": "application/json"
  });
  return fetch(url, options);
}

// Quick View Modal
document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".quick-view").forEach(btn => {
    btn.addEventListener("click", async () => {
      const candidateId = btn.dataset.id;
      const response = await authFetch(`/candidate/${candidateId}`);
      const data = await response.text();
      document.getElementById("quickViewContent").innerHTML = data;
      const modal = new bootstrap.Modal(document.getElementById("quickViewModal"));
      modal.show();
    });
  });

  // Search form submit
  const searchForm = document.getElementById("searchForm");
  if (searchForm) {
    searchForm.addEventListener("submit", (e) => {
      e.preventDefault();
      const query = document.getElementById("searchInput").value;
      window.location.href = `/candidates?search=${encodeURIComponent(query)}`;
    });
  }
});
