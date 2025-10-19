const $ = (selector) => document.querySelector(selector);

const statusOutput = $("#status-output");
const fetcherList = $("#fetcher-list");
const graphError = $("#graph-error");
const tablesOutput = $("#tables-output");
const readinessOutput = $("#readiness-output");
const syncFetcherInput = $("#sync-fetcher");
const syncQueryInput = $("#sync-query");
const syncBudgetType = $("#sync-budget-type");
const syncBudgetValue = $("#sync-budget-value");
const syncParamsInput = $("#sync-params");
const syncTargetsInput = $("#sync-targets");
const syncOutput = $("#sync-output");
let network = null;

async function fetchJSON(url, options) {
    const response = await fetch(url, options);
    if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status} ${response.statusText} - ${text}`);
    }
    return response.json();
}

async function loadStatus() {
    try {
        const data = await fetchJSON("/api/status");
        statusOutput.textContent = JSON.stringify(data, null, 2);
    } catch (error) {
        statusOutput.textContent = `加载失败: ${error.message}`;
    }
}

function renderFetcher(fetcher) {
    const card = document.createElement("div");
    card.className = "fetcher-card";
    const produces = fetcher.produces
        .map((item) => `${item.kind}:${item.name}`)
        .join(", ");
    card.innerHTML = `
        <h3>${fetcher.name}</h3>
        <div class="fetcher-meta">${fetcher.description}</div>
        <div class="fetcher-meta">默认 TTL: ${fetcher.default_ttl_secs ?? "未指定"}</div>
        <div class="fetcher-meta">产出: ${produces || "无"}</div>
        <pre class="json-output">${JSON.stringify(fetcher.param_schema, null, 2)}</pre>
    `;
    return card;
}

async function loadFetchers() {
    fetcherList.innerHTML = "加载中…";
    try {
        const fetchers = await fetchJSON("/api/fetchers");
        if (!fetchers.length) {
            fetcherList.textContent = "尚未注册 fetcher。";
            return;
        }
        fetcherList.innerHTML = "";
        fetchers.forEach((fetcher) => fetcherList.appendChild(renderFetcher(fetcher)));
        const datalist = document.getElementById("fetcher-options");
        if (datalist) {
            datalist.innerHTML = "";
            fetchers.forEach((fetcher) => {
                const option = document.createElement("option");
                option.value = fetcher.name;
                datalist.appendChild(option);
            });
        }
        if (syncFetcherInput && fetchers.length && !syncFetcherInput.value) {
            syncFetcherInput.value = fetchers[0].name;
        }
    } catch (error) {
        fetcherList.textContent = `加载失败: ${error.message}`;
    }
}

function buildGraphData(graphJson) {
    const nodes = (graphJson.nodes || []).map((node) => {
        const title =
            node.props?.name ||
            node.props?.label ||
            node.props?.title ||
            node.label ||
            String(node.id);
        return {
            id: node.id,
            label: title,
            group: node.label || "Node",
        };
    });

    const edges = (graphJson.edges || []).map((edge) => ({
        from: edge.from_id ?? edge.from ?? edge.source,
        to: edge.to_id ?? edge.to ?? edge.target,
        label: edge.label || edge.type || "",
    }));

    return { nodes, edges };
}

function renderGraph(graphJson) {
    const container = document.getElementById("graph-container");
    const { nodes, edges } = buildGraphData(graphJson);

    if (!network) {
        network = new vis.Network(container, { nodes, edges }, {
            nodes: {
                shape: "dot",
                scaling: { min: 8, max: 24 },
                font: { color: "#e2e8f0" },
            },
            edges: {
                arrows: "to",
                color: { color: "#64748b" },
                font: { color: "#94a3b8", strokeWidth: 0 },
                smooth: true,
            },
            physics: {
                stabilization: true,
                barnesHut: {
                    gravitationalConstant: -2000,
                    springLength: 140,
                },
            },
        });
    } else {
        network.setData({ nodes: new vis.DataSet(nodes), edges: new vis.DataSet(edges) });
    }
}

async function loadGraph() {
    graphError.textContent = "";
    try {
        const k = Number($("#graph-limit").value) || 150;
        const nodeProp = $("#graph-node-prop").value.trim();
        const params = new URLSearchParams();
        params.set("k", Math.min(Math.max(k, 10), 300));
        if (nodeProp) params.set("node_prop", nodeProp);

        const graphJson = await fetchJSON(`/api/graph/visual?${params.toString()}`);
        renderGraph(graphJson);
    } catch (error) {
        graphError.textContent = `图数据加载失败: ${error.message}`;
    }
}

function renderTable(table) {
    const card = document.createElement("div");
    card.className = "table-card";
    const columns = (table.columns || [])
        .map((col) => `${col.name}:${col.data_type}${col.nullable ? "" : " (not null)"}`)
        .join(", ");
    card.innerHTML = `
        <h4>${table.table_path}</h4>
        <div class="meta">${columns || "无列信息"}</div>
    `;
    return card;
}

async function loadTables() {
    tablesOutput.textContent = "加载中…";
    try {
        const prefix = $("#table-prefix").value.trim();
        const query = prefix ? `?prefix=${encodeURIComponent(prefix)}` : "";
        const tables = await fetchJSON(`/api/tables${query}`);
        if (!tables.length) {
            tablesOutput.textContent = "未查询到表。";
            return;
        }
        tablesOutput.innerHTML = "";
        tables.forEach((table) => tablesOutput.appendChild(renderTable(table)));
    } catch (error) {
        tablesOutput.textContent = `查询失败: ${error.message}`;
    }
}

async function runReadiness() {
    readinessOutput.textContent = "请求中…";
    try {
        const text = $("#readiness-input").value.trim();
        const payload = text ? JSON.parse(text) : [];
        if (!Array.isArray(payload)) {
            throw new Error("输入必须是 JSON 数组");
        }
        const data = await fetchJSON("/api/readiness", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });
        readinessOutput.textContent = JSON.stringify(data, null, 2);
    } catch (error) {
        readinessOutput.textContent = `探测失败: ${error.message}`;
    }
}

async function runSync() {
    const button = $("#run-sync");
    if (!button) return;
    const originalLabel = button.textContent;
    button.disabled = true;
    button.textContent = "执行中…";
    if (syncOutput) syncOutput.textContent = "执行中…";
    try {
        const fetcher = (syncFetcherInput?.value || "").trim();
        if (!fetcher) {
            throw new Error("请填写 fetcher 名称");
        }

        let params = {};
        if (syncParamsInput) {
            const raw = syncParamsInput.value.trim();
            if (raw) {
                try {
                    params = JSON.parse(raw);
                } catch (error) {
                    throw new Error(`Fetch params JSON 解析失败: ${error.message}`);
                }
            }
        }

        let targets = [];
        if (syncTargetsInput) {
            const rawTargets = syncTargetsInput.value.trim();
            if (rawTargets) {
                try {
                    const parsed = JSON.parse(rawTargets);
                    if (!Array.isArray(parsed)) {
                        throw new Error("Target entities 必须是 JSON 数组");
                    }
                    targets = parsed;
                } catch (error) {
                    throw new Error(`Target entities 解析失败: ${error.message}`);
                }
            }
        }

        const budgetType = syncBudgetType?.value || "request_count";
        const budgetValue = Number(syncBudgetValue?.value) || 0;
        const budget =
            budgetType === "duration_secs"
                ? {
                      type: "duration_secs",
                      seconds: Math.max(1, Math.floor(budgetValue) || 60),
                  }
                : {
                      type: "request_count",
                      count: Math.max(1, Math.floor(budgetValue) || 100),
                  };

        const payload = {
            fetcher,
            params,
            budget,
        };

        const triggeringQuery = (syncQueryInput?.value || "").trim();
        if (triggeringQuery) {
            payload.triggering_query = triggeringQuery;
        }
        if (targets.length) {
            payload.target_entities = targets;
        }

        const result = await fetchJSON("/api/sync", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        });

        if (syncOutput) {
            syncOutput.textContent = JSON.stringify(result, null, 2);
        }

        await loadStatus();
    } catch (error) {
        if (syncOutput) {
            syncOutput.textContent = `同步失败: ${error.message}`;
        }
    } finally {
        button.disabled = false;
        button.textContent = originalLabel;
    }
}

function attachEventHandlers() {
    $("#refresh-all").addEventListener("click", () => {
        loadStatus();
        loadFetchers();
        loadGraph();
        loadTables();
    });
    $("#refresh-status").addEventListener("click", loadStatus);
    $("#refresh-fetchers").addEventListener("click", loadFetchers);
    $("#refresh-graph").addEventListener("click", loadGraph);
    $("#refresh-tables").addEventListener("click", loadTables);
    $("#run-readiness").addEventListener("click", runReadiness);
    $("#run-sync").addEventListener("click", runSync);
}

async function bootstrap() {
    attachEventHandlers();
    await Promise.all([loadStatus(), loadFetchers(), loadGraph(), loadTables()]);
    if (syncOutput) {
        syncOutput.textContent = "等待执行...";
    }
}

document.addEventListener("DOMContentLoaded", bootstrap);
