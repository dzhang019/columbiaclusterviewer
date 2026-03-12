const metricGrid = document.getElementById("metric-grid");
const queueStatus = document.getElementById("queue-status");
const topUsers = document.getElementById("top-users");
const myJobsTable = document.getElementById("my-jobs-table");
const nodesTable = document.getElementById("nodes-table");
const diagnostics = document.getElementById("diagnostics");

function renderMetrics(metrics) {
  metricGrid.innerHTML = metrics
    .map(
      (metric) => `
        <article class="metric-card metric-card--${metric.status}">
          <span class="metric-label">${metric.label}</span>
          <strong class="metric-value">${metric.value}</strong>
          <span class="metric-detail">${metric.detail || "&nbsp;"}</span>
        </article>
      `,
    )
    .join("");
}

function renderPills(target, items, emptyLabel, keyLabel, valueLabel) {
  if (!items.length) {
    target.innerHTML = `<span class="empty">${emptyLabel}</span>`;
    return;
  }

  target.innerHTML = items
    .map(
      (item) => `
        <div class="pill">
          <span>${item[keyLabel]}</span>
          <strong>${item[valueLabel]}</strong>
        </div>
      `,
    )
    .join("");
}

function renderRows(target, rows, keys, emptyLabel) {
  if (!rows.length) {
    target.innerHTML = `<tr><td colspan="${keys.length}">${emptyLabel}</td></tr>`;
    return;
  }

  target.innerHTML = rows
    .map(
      (row) => `
        <tr>${keys.map((key) => `<td>${row[key] || ""}</td>`).join("")}</tr>
      `,
    )
    .join("");
}

function renderDiagnostics(scheduler) {
  diagnostics.textContent = JSON.stringify(scheduler.commands, null, 2);
}

async function refreshDashboard() {
  try {
    const response = await fetch("/api/dashboard", { cache: "no-store" });
    const data = await response.json();

    document.getElementById("host-name").textContent = data.system.hostname;
    document.getElementById("generated-at").textContent = new Date(data.generated_at).toLocaleString();
    document.getElementById("current-user").textContent = data.scheduler.current_user || "";

    renderMetrics(data.metrics);

    renderPills(
      queueStatus,
      Object.entries(data.scheduler.queue_status).map(([status, jobs]) => ({ status, jobs })),
      "No scheduler queue data.",
      "status",
      "jobs",
    );

    renderPills(topUsers, data.top_users, "No active jobs.", "user", "jobs");
    renderRows(
      myJobsTable,
      data.scheduler.my_jobs,
      ["job_id", "state", "partition", "elapsed", "nodes", "reason"],
      "No jobs for the current user.",
    );
    renderRows(
      nodesTable,
      data.scheduler.nodes.map((node) => ({
        name: node.name,
        state: node.state,
        cpu: `${node.cpu_allocated}/${node.cpu_idle}/${node.cpu_total}`,
        memory_mb: node.memory_mb,
        features: node.features,
      })),
      ["name", "state", "cpu", "memory_mb", "features"],
      "No node data from scheduler.",
    );
    renderDiagnostics(data.scheduler);
  } catch (error) {
    diagnostics.textContent = `Failed to load dashboard data: ${error}`;
  }
}

refreshDashboard();
window.setInterval(refreshDashboard, 30000);
