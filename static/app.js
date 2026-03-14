const metricGrid = document.getElementById("metric-grid");
const queueStatus = document.getElementById("queue-status");
const clusterStatus = document.getElementById("cluster-status");
const topUsers = document.getElementById("top-users");
const jobsTable = document.getElementById("jobs-table");
const nodesTable = document.getElementById("nodes-table");
const diagnostics = document.getElementById("diagnostics");
const overviewUserFilter = document.getElementById("overview-user-filter");
const nodesFilter = document.getElementById("nodes-filter");
const diagnosticsUserFilter = document.getElementById("diagnostics-user-filter");
const diagnosticsNodeFilter = document.getElementById("diagnostics-node-filter");
const diagnosticsUserFilterState = document.getElementById("diagnostics-user-filter-state");
const diagnosticsNodeFilterState = document.getElementById("diagnostics-node-filter-state");
const jobCount = document.getElementById("job-count");
const nodeCount = document.getElementById("node-count");
const nodeStatus = document.getElementById("node-status");
const nodeNotes = document.getElementById("node-notes");
const historyCoverage = document.getElementById("history-coverage");
const historyRangeLabel = document.getElementById("history-range-label");
const filterSummary = document.getElementById("filter-summary");
const rangePicker = document.getElementById("range-picker");
const nodeScope = document.getElementById("node-scope");
const tabBar = document.getElementById("tab-bar");

let latestDashboard = null;
let activeRange = "1h";
let activeTab = "overview";
let activeNodeScope = "all";
let filterTimer = null;

const TAB_IDS = ["overview", "nodes", "diagnostics"];
const TAB_HASHES = {
  overview: "#view-overview",
  nodes: "#view-nodes",
  diagnostics: "#view-diagnostics",
};
const JOB_LIMIT = 25;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function setTab(tabId, updateHash = true) {
  activeTab = TAB_IDS.includes(tabId) ? tabId : "overview";
  for (const button of tabBar.querySelectorAll(".tab-button")) {
    button.classList.toggle("is-active", button.dataset.tab === activeTab);
  }
  for (const panel of document.querySelectorAll("[data-tabpanel]")) {
    const isActive = panel.dataset.tabpanel === activeTab;
    panel.classList.toggle("is-active", isActive);
    panel.hidden = !isActive;
  }
  if (updateHash) {
    window.history.replaceState(null, "", TAB_HASHES[activeTab]);
  }
}

function setRange(rangeKey) {
  activeRange = rangeKey;
  const labelMap = {
    live: "Live",
    "5m": "Last 5m",
    "1h": "Last 1h",
    "1d": "Last 1d",
    "1mo": "Last 1mo",
  };
  historyRangeLabel.textContent = labelMap[rangeKey] || "Last 1h";
  for (const button of rangePicker.querySelectorAll(".range-chip")) {
    button.classList.toggle("is-active", button.dataset.range === rangeKey);
  }
}

function setNodeScope(scope) {
  activeNodeScope = scope;
  for (const button of nodeScope.querySelectorAll(".range-chip")) {
    button.classList.toggle("is-active", button.dataset.nodeScope === scope);
  }
}

function renderMetrics(metrics) {
  metricGrid.innerHTML = metrics
    .map(
      (metric) => `
        <article class="metric-card metric-card--${escapeHtml(metric.status)}">
          <span class="metric-label">${escapeHtml(metric.label)}</span>
          <strong class="metric-value">${escapeHtml(metric.value)}</strong>
          <span class="metric-detail">${escapeHtml(metric.detail || "\u00a0")}</span>
        </article>
      `,
    )
    .join("");
}

function renderPills(target, items, emptyLabel, keyLabel, valueLabel) {
  if (!items.length) {
    target.innerHTML = `<span class="empty">${escapeHtml(emptyLabel)}</span>`;
    return;
  }

  target.innerHTML = items
    .map(
      (item) => `
        <div class="pill">
          <span>${escapeHtml(item[keyLabel])}</span>
          <strong>${escapeHtml(item[valueLabel])}</strong>
        </div>
      `,
    )
    .join("");
}

function renderRows(target, rows, keys, emptyLabel, rowClassBuilder = null) {
  if (!rows.length) {
    target.innerHTML = `<tr><td colspan="${keys.length}">${escapeHtml(emptyLabel)}</td></tr>`;
    return;
  }

  target.innerHTML = rows
    .map((row) => {
      const rowClass = rowClassBuilder ? rowClassBuilder(row) : "";
      const classAttr = rowClass ? ` class="${escapeHtml(rowClass)}"` : "";
      return `<tr${classAttr}>${keys
        .map((key) => `<td>${escapeHtml(row[key] || "")}</td>`)
        .join("")}</tr>`;
    })
    .join("");
}

function renderDiagnostics(scheduler) {
  diagnostics.textContent = JSON.stringify(scheduler.commands, null, 2);
}

function renderChart(targetId, seriesList, emptyLabel) {
  const target = document.getElementById(targetId);
  const activeSeries = seriesList.filter((series) => series.values.length);

  if (!activeSeries.length) {
    target.innerHTML = `<span class="empty">${escapeHtml(emptyLabel)}</span>`;
    return;
  }

  const width = 480;
  const height = 180;
  const padding = 18;
  const allValues = activeSeries.flatMap((series) => series.values);
  const maxValue = Math.max(...allValues, 1);

  const polylines = activeSeries
    .map((series) => {
      const points = series.values
        .map((value, index) => {
          const x = padding + ((width - padding * 2) * index) / Math.max(series.values.length - 1, 1);
          const y = height - padding - (value / maxValue) * (height - padding * 2);
          return `${x},${y}`;
        })
        .join(" ");
      return `<polyline fill="none" stroke="${series.color}" stroke-width="3" points="${points}" />`;
    })
    .join("");

  const legend = activeSeries
    .map(
      (series) => `
        <div class="legend-item">
          <span class="legend-swatch" style="background:${escapeHtml(series.color)}"></span>
          <span>${escapeHtml(series.label)}</span>
        </div>
      `,
    )
    .join("");

  const summaries = activeSeries
    .map((series) => {
      const current = series.values[series.values.length - 1] ?? 0;
      const low = Math.min(...series.values);
      const high = Math.max(...series.values);
      return `
        <div class="chart-stat">
          <strong>${escapeHtml(series.label)}</strong>
          <span>Current ${escapeHtml(formatSeriesValue(current, series.unit))}</span>
          <span>Low ${escapeHtml(formatSeriesValue(low, series.unit))}</span>
          <span>High ${escapeHtml(formatSeriesValue(high, series.unit))}</span>
        </div>
      `;
    })
    .join("");

  target.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" class="chart-svg" aria-hidden="true">
      <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" class="chart-axis"></line>
      <line x1="${padding}" y1="${padding}" x2="${padding}" y2="${height - padding}" class="chart-axis"></line>
      ${polylines}
    </svg>
    <div class="chart-footer">
      <div class="chart-stats">${summaries}</div>
      <div class="legend">${legend}</div>
    </div>
  `;
}

function formatSeriesValue(value, unit = "") {
  if (unit === "%") {
    return `${value.toFixed(1)}%`;
  }
  return value.toFixed(1);
}

function getStatePriority(state) {
  const normalized = (state || "").toUpperCase();
  if (normalized.startsWith("PD")) {
    return 0;
  }
  if (normalized.startsWith("CF")) {
    return 1;
  }
  if (normalized.startsWith("R")) {
    return 2;
  }
  return 3;
}

function isProblemNode(node) {
  const state = (node.state || "").toLowerCase();
  return state.includes("down") || state.includes("drain") || state.includes("fail");
}

function isGpuNode(node) {
  const haystack = `${node.name || ""} ${node.features || ""}`.toLowerCase();
  return haystack.includes("gpu");
}

function getNodePriority(state) {
  const normalized = (state || "").toLowerCase();
  if (normalized.includes("down") || normalized.includes("fail")) {
    return 0;
  }
  if (normalized.includes("drain")) {
    return 1;
  }
  if (normalized.includes("alloc") || normalized.includes("mix")) {
    return 2;
  }
  if (normalized.includes("idle")) {
    return 3;
  }
  return 4;
}

function sortJobs(jobs) {
  return [...jobs].sort((left, right) => {
    const priority = getStatePriority(left.state) - getStatePriority(right.state);
    if (priority !== 0) {
      return priority;
    }
    return String(left.job_id).localeCompare(String(right.job_id));
  });
}

function sortNodes(nodes) {
  return [...nodes].sort((left, right) => {
    const priority = getNodePriority(left.state) - getNodePriority(right.state);
    if (priority !== 0) {
      return priority;
    }
    return left.name.localeCompare(right.name);
  });
}

function renderHistory(history) {
  const cluster = history.cluster || [];
  const userSeries = history.user || [];
  const nodeSeries = history.node || [];

  renderChart(
    "chart-load",
    [{ label: "Load", color: "#0c6a58", values: cluster.map((point) => point.load1 || 0), unit: "" }],
    "Waiting for load samples.",
  );
  renderChart(
    "chart-memory",
    [{ label: "Memory", color: "#9d5c12", values: cluster.map((point) => point.memory_percent || 0), unit: "%" }],
    "Waiting for memory samples.",
  );
  renderChart(
    "chart-cpu",
    [
      { label: "Allocated", color: "#0c6a58", values: cluster.map((point) => point.cpu_allocated || 0), unit: "" },
      { label: "Idle", color: "#758579", values: cluster.map((point) => point.cpu_idle || 0), unit: "" },
    ],
    "Waiting for CPU samples.",
  );
  renderChart(
    "chart-queue",
    [
      { label: "Running", color: "#0c6a58", values: cluster.map((point) => point.running_jobs || 0), unit: "" },
      { label: "Pending", color: "#d18a2d", values: cluster.map((point) => point.pending_jobs || 0), unit: "" },
    ],
    "Waiting for queue samples.",
  );
  renderChart(
    "chart-user",
    [
      { label: "Running", color: "#0c6a58", values: userSeries.map((point) => point.running_jobs || 0), unit: "" },
      { label: "Pending", color: "#d18a2d", values: userSeries.map((point) => point.pending_jobs || 0), unit: "" },
      { label: "Total", color: "#294b43", values: userSeries.map((point) => point.total_jobs || 0), unit: "" },
    ],
    "Add a user filter to see matching user history.",
  );
  renderChart(
    "chart-node",
    [
      { label: "Allocated CPU", color: "#0c6a58", values: nodeSeries.map((point) => point.cpu_allocated || 0), unit: "" },
      { label: "Idle CPU", color: "#758579", values: nodeSeries.map((point) => point.cpu_idle || 0), unit: "" },
    ],
    "Add a node filter to see matching node history.",
  );

  if (history.coverage && history.coverage.samples) {
    historyCoverage.textContent = `${history.coverage.samples} samples from ${new Date(history.coverage.oldest).toLocaleString()} to ${new Date(history.coverage.newest).toLocaleString()}`;
  } else {
    historyCoverage.textContent = "No historical samples yet";
  }

  renderDiagnosticsFilterState(history);
}

function setFilterVisualState(input, stateEl, tone, text) {
  input.classList.remove("filter-input--active", "filter-input--invalid");
  stateEl.classList.remove("filter-state--active", "filter-state--invalid");

  if (tone === "active") {
    input.classList.add("filter-input--active");
    stateEl.classList.add("filter-state--active");
  } else if (tone === "invalid") {
    input.classList.add("filter-input--invalid");
    stateEl.classList.add("filter-state--invalid");
  }

  stateEl.textContent = text;
}

function formatMatchSummary(label, matches) {
  if (!matches.length) {
    return `${label}: no matches`;
  }
  if (matches.length === 1) {
    return `${label}: ${matches[0]}`;
  }
  const preview = matches.slice(0, 3).join(", ");
  const suffix = matches.length > 3 ? ` +${matches.length - 3} more` : "";
  return `${label}: ${matches.length} matches (${preview}${suffix})`;
}

function getMatchedUsers(filterValue) {
  const users = new Set();
  for (const job of latestDashboard.scheduler.jobs || []) {
    if (!filterValue || job.user.toLowerCase().includes(filterValue.toLowerCase())) {
      users.add(job.user);
    }
  }
  return [...users].sort((left, right) => left.localeCompare(right));
}

function getMatchedNodes(filterValue) {
  const nodes = new Set();
  for (const node of latestDashboard.scheduler.nodes || []) {
    const haystack = `${node.name} ${node.features}`.toLowerCase();
    if (!filterValue || haystack.includes(filterValue.toLowerCase())) {
      nodes.add(node.name);
    }
  }
  return [...nodes].sort((left, right) => left.localeCompare(right));
}

function renderDiagnosticsFilterState(history) {
  const userValue = diagnosticsUserFilter.value.trim();
  const nodeValue = diagnosticsNodeFilter.value.trim();
  const userHasMatches = (history.user || []).some((point) => (point.total_jobs || 0) > 0);
  const nodeHasMatches = (history.node || []).some((point) => (point.matched_nodes || 0) > 0 || (point.cpu_total || 0) > 0);
  const matchedUsers = userValue ? getMatchedUsers(userValue) : [];
  const matchedNodes = nodeValue ? getMatchedNodes(nodeValue) : [];

  if (!userValue) {
    setFilterVisualState(diagnosticsUserFilter, diagnosticsUserFilterState, "neutral", "All users");
  } else if (userHasMatches) {
    setFilterVisualState(
      diagnosticsUserFilter,
      diagnosticsUserFilterState,
      "active",
      matchedUsers.length ? formatMatchSummary("Users", matchedUsers) : "Active match",
    );
  } else {
    setFilterVisualState(diagnosticsUserFilter, diagnosticsUserFilterState, "invalid", "No matches");
  }

  if (!nodeValue) {
    setFilterVisualState(diagnosticsNodeFilter, diagnosticsNodeFilterState, "neutral", "All nodes");
  } else if (nodeHasMatches) {
    setFilterVisualState(
      diagnosticsNodeFilter,
      diagnosticsNodeFilterState,
      "active",
      matchedNodes.length ? formatMatchSummary("Nodes", matchedNodes) : "Active match",
    );
  } else {
    setFilterVisualState(diagnosticsNodeFilter, diagnosticsNodeFilterState, "invalid", "No matches");
  }

  if (userValue || nodeValue) {
    const userSummary = userValue ? (matchedUsers.length ? formatMatchSummary("users", matchedUsers) : "users: no matches") : "users: all";
    const nodeSummary = nodeValue ? (matchedNodes.length ? formatMatchSummary("nodes", matchedNodes) : "nodes: no matches") : "nodes: all";
    filterSummary.textContent = `${userSummary} | ${nodeSummary}`;
  } else {
    filterSummary.textContent = "Subset matching the active history filters";
  }
}

function renderOverview() {
  const jobs = sortJobs(
    latestDashboard.scheduler.jobs.filter((job) => {
      const userValue = overviewUserFilter.value.trim().toLowerCase();
      return !userValue || job.user.toLowerCase().includes(userValue);
    }),
  ).slice(0, JOB_LIMIT);

  jobCount.textContent = `${jobs.length} of ${latestDashboard.scheduler.jobs.length} jobs shown`;
  renderRows(
    jobsTable,
    jobs,
    ["job_id", "state", "user", "partition", "elapsed", "nodes", "reason"],
    overviewUserFilter.value.trim() ? "No jobs match that user filter." : "No scheduler jobs found.",
    (row) => (getStatePriority(row.state) < 2 ? "row--attention" : ""),
  );

  renderPills(
    queueStatus,
    Object.entries(latestDashboard.scheduler.queue_status).map(([status, jobsCount]) => ({ status, jobs: jobsCount })),
    "No scheduler queue data.",
    "status",
    "jobs",
  );

  const nodeSummary = latestDashboard.scheduler.nodes.reduce(
    (summary, node) => {
      if (isProblemNode(node)) {
        summary.problem += 1;
      }
      if ((node.state || "").toLowerCase().includes("idle")) {
        summary.idle += 1;
      }
      if ((node.state || "").toLowerCase().includes("alloc") || (node.state || "").toLowerCase().includes("mix")) {
        summary.allocated += 1;
      }
      return summary;
    },
    { allocated: 0, idle: 0, problem: 0 },
  );

  renderPills(
    clusterStatus,
    [
      { label: "Allocated", value: nodeSummary.allocated },
      { label: "Idle", value: nodeSummary.idle },
      { label: "Problem", value: nodeSummary.problem },
    ],
    "No node status data.",
    "label",
    "value",
  );

  renderPills(topUsers, latestDashboard.top_users, "No active jobs.", "user", "jobs");
}

function renderNodes() {
  const filterValue = nodesFilter.value.trim().toLowerCase();
  const nodes = sortNodes(
    latestDashboard.scheduler.nodes.filter((node) => {
      const matchesText =
        !filterValue ||
        node.name.toLowerCase().includes(filterValue) ||
        node.features.toLowerCase().includes(filterValue);
      if (!matchesText) {
        return false;
      }
      if (activeNodeScope === "problem") {
        return isProblemNode(node);
      }
      if (activeNodeScope === "gpu") {
        return isGpuNode(node);
      }
      return true;
    }),
  );

  nodeCount.textContent = `${nodes.length} of ${latestDashboard.scheduler.nodes.length} nodes shown`;
  renderRows(
    nodesTable,
    nodes.map((node) => ({
      name: node.name,
      state: node.state,
      cpu: `${node.cpu_allocated}/${node.cpu_idle}/${node.cpu_total}`,
      memory_mb: node.memory_mb,
      features: node.features,
    })),
    ["name", "state", "cpu", "memory_mb", "features"],
    filterValue || activeNodeScope !== "all" ? "No nodes match the current filters." : "No node data from scheduler.",
    (row) => (isProblemNode(row) ? "row--problem" : ""),
  );

  const counts = nodes.reduce(
    (summary, node) => {
      if (isProblemNode(node)) {
        summary.problem += 1;
      }
      if ((node.state || "").toLowerCase().includes("idle")) {
        summary.idle += 1;
      }
      if ((node.state || "").toLowerCase().includes("alloc") || (node.state || "").toLowerCase().includes("mix")) {
        summary.allocated += 1;
      }
      if (isGpuNode(node)) {
        summary.gpu += 1;
      }
      return summary;
    },
    { allocated: 0, idle: 0, problem: 0, gpu: 0 },
  );

  renderPills(
    nodeStatus,
    [
      { label: "Allocated", value: counts.allocated },
      { label: "Idle", value: counts.idle },
      { label: "Problem", value: counts.problem },
      { label: "GPU Nodes", value: counts.gpu },
    ],
    "No node summary data.",
    "label",
    "value",
  );

  const notes = [];
  if (counts.problem > 0) {
    notes.push(`${counts.problem} node(s) are in a problem state.`);
  }
  if (counts.gpu > 0) {
    notes.push(`${counts.gpu} visible node(s) look GPU-capable based on name/features.`);
  }
  if (!notes.length) {
    notes.push("No obvious node issues are visible in the current sample.");
  }
  nodeNotes.innerHTML = notes.map((note) => `<div class="note-item">${escapeHtml(note)}</div>`).join("");
}

async function refreshDashboard() {
  try {
    const params = new URLSearchParams({
      range: activeRange,
      user: diagnosticsUserFilter.value.trim(),
      node: diagnosticsNodeFilter.value.trim(),
    });
    const response = await fetch(`/api/dashboard?${params.toString()}`, { cache: "no-store" });
    const data = await response.json();
    latestDashboard = data;

    document.getElementById("host-name").textContent = data.system.hostname;
    document.getElementById("generated-at").textContent = new Date(data.generated_at).toLocaleString();

    renderMetrics(data.metrics);
    renderOverview();
    renderNodes();
    renderHistory(data.history || {});
    renderDiagnostics(data.scheduler);
  } catch (error) {
    diagnostics.textContent = `Failed to load dashboard data: ${error}`;
  }
}

function scheduleRefresh() {
  if (!latestDashboard) {
    return;
  }
  renderOverview();
  renderNodes();
  window.clearTimeout(filterTimer);
  filterTimer = window.setTimeout(refreshDashboard, 250);
}

overviewUserFilter.addEventListener("input", renderOverview);
nodesFilter.addEventListener("input", renderNodes);
diagnosticsUserFilter.addEventListener("input", scheduleRefresh);
diagnosticsNodeFilter.addEventListener("input", scheduleRefresh);

rangePicker.addEventListener("click", (event) => {
  const button = event.target.closest(".range-chip");
  if (!button) {
    return;
  }
  setRange(button.dataset.range);
  refreshDashboard();
});

nodeScope.addEventListener("click", (event) => {
  const button = event.target.closest(".range-chip");
  if (!button) {
    return;
  }
  setNodeScope(button.dataset.nodeScope);
  renderNodes();
});

tabBar.addEventListener("click", (event) => {
  const button = event.target.closest(".tab-button");
  if (!button) {
    return;
  }
  setTab(button.dataset.tab);
});

window.addEventListener("hashchange", () => {
  const tabId = Object.entries(TAB_HASHES).find(([, hash]) => hash === window.location.hash)?.[0] || "overview";
  setTab(tabId, false);
});

setRange(activeRange);
setNodeScope(activeNodeScope);
setTab(Object.entries(TAB_HASHES).find(([, hash]) => hash === window.location.hash)?.[0] || "overview", false);
refreshDashboard();
window.setInterval(refreshDashboard, 30000);
