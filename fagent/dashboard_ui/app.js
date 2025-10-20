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
const graphLegend = $("#graph-legend");
const graphStartSelect = $("#graph-start-node");
const graphDepthInput = $("#graph-depth");
const graphNodeLimitInput = $("#graph-node-limit");
const graphEdgeTypesInput = $("#graph-edge-types");
let graphCandidates = [];

const TYPE_ALIASES = {
    PROJECT: "Project",
    VERSION: "Version",
    COMMIT: "Commit",
    FILE: "File",
    DIRECTORY: "Directory",
    MODULE: "Module",
    PACKAGE: "Package",
    CLASS: "Class",
    STRUCT: "Struct",
    ENUM: "Enum",
    TRAIT: "Trait",
    INTERFACE: "Interface",
    FUNCTION: "Function",
    METHOD: "Method",
    CALL: "Call",
    README_CHUNK: "ReadmeChunk",
    CODE_CHUNK: "CodeChunk",
    README: "ReadmeChunk",
    VECTOR: "Vector",
    EMBEDDING: "Vector",
};

const NODE_TYPE_STYLES = {
    Project: {
        displayName: "Project 项目",
        color: {
            background: "#0ea5e9",
            border: "#38bdf8",
            highlight: { background: "#38bdf8", border: "#0ea5e9" },
            hover: { background: "#38bdf8", border: "#0ea5e9" },
        },
        fontColor: "#0f172a",
    },
    Version: {
        displayName: "Version 版本",
        color: {
            background: "#22d3ee",
            border: "#67e8f9",
            highlight: { background: "#67e8f9", border: "#22d3ee" },
            hover: { background: "#67e8f9", border: "#22d3ee" },
        },
        fontColor: "#0f172a",
    },
    Commit: {
        displayName: "Commit 提交",
        color: {
            background: "#f97316",
            border: "#fb923c",
            highlight: { background: "#fb923c", border: "#f97316" },
            hover: { background: "#fb923c", border: "#f97316" },
        },
        fontColor: "#0f172a",
    },
    File: {
        displayName: "File 文件",
        color: {
            background: "#6366f1",
            border: "#818cf8",
            highlight: { background: "#818cf8", border: "#6366f1" },
            hover: { background: "#818cf8", border: "#6366f1" },
        },
        fontColor: "#0f172a",
    },
    Module: {
        displayName: "Module 模块",
        color: {
            background: "#8b5cf6",
            border: "#a855f7",
            highlight: { background: "#a855f7", border: "#8b5cf6" },
            hover: { background: "#a855f7", border: "#8b5cf6" },
        },
        fontColor: "#0f172a",
    },
    Class: {
        displayName: "Class 类",
        color: {
            background: "#14b8a6",
            border: "#2dd4bf",
            highlight: { background: "#2dd4bf", border: "#14b8a6" },
            hover: { background: "#2dd4bf", border: "#14b8a6" },
        },
        fontColor: "#0f172a",
    },
    Struct: {
        displayName: "Struct 结构体",
        color: {
            background: "#0f766e",
            border: "#14b8a6",
            highlight: { background: "#14b8a6", border: "#0f766e" },
            hover: { background: "#14b8a6", border: "#0f766e" },
        },
        fontColor: "#e0f2f1",
    },
    Trait: {
        displayName: "Trait 特征",
        color: {
            background: "#059669",
            border: "#34d399",
            highlight: { background: "#34d399", border: "#059669" },
            hover: { background: "#34d399", border: "#059669" },
        },
        fontColor: "#0f172a",
    },
    Function: {
        displayName: "Function 函数",
        color: {
            background: "#ef4444",
            border: "#f87171",
            highlight: { background: "#f87171", border: "#ef4444" },
            hover: { background: "#f87171", border: "#ef4444" },
        },
        fontColor: "#0f172a",
    },
    Method: {
        displayName: "Method 方法",
        color: {
            background: "#dc2626",
            border: "#f87171",
            highlight: { background: "#f87171", border: "#dc2626" },
            hover: { background: "#f87171", border: "#dc2626" },
        },
        fontColor: "#f8fafc",
    },
    ReadmeChunk: {
        displayName: "README 片段",
        color: {
            background: "#facc15",
            border: "#fde047",
            highlight: { background: "#fde047", border: "#facc15" },
            hover: { background: "#fde047", border: "#facc15" },
        },
        fontColor: "#0f172a",
    },
    CodeChunk: {
        displayName: "Code 片段",
        color: {
            background: "#22c55e",
            border: "#4ade80",
            highlight: { background: "#4ade80", border: "#22c55e" },
            hover: { background: "#4ade80", border: "#22c55e" },
        },
        fontColor: "#0f172a",
    },
    Vector: {
        displayName: "向量表示",
        color: {
            background: "#e879f9",
            border: "#f0abfc",
            highlight: { background: "#f0abfc", border: "#e879f9" },
            hover: { background: "#f0abfc", border: "#e879f9" },
        },
        fontColor: "#3b0764",
    },
    Default: {
        displayName: "其他",
        color: {
            background: "#475569",
            border: "#94a3b8",
            highlight: { background: "#94a3b8", border: "#475569" },
            hover: { background: "#94a3b8", border: "#475569" },
        },
        fontColor: "#e2e8f0",
    },
};

function normalizeType(label) {
    if (!label) return "Default";
    const stringified = String(label);
    if (NODE_TYPE_STYLES[stringified]) {
        return stringified;
    }
    const upper = stringified.toUpperCase();
    if (TYPE_ALIASES[upper]) {
        return TYPE_ALIASES[upper];
    }
    const title =
        stringified.length > 1
            ? stringified[0].toUpperCase() + stringified.slice(1).toLowerCase()
            : stringified.toUpperCase();
    if (NODE_TYPE_STYLES[title]) {
        return title;
    }
    return "Default";
}

function getNodeStyle(nodeType) {
    return NODE_TYPE_STYLES[nodeType] ?? NODE_TYPE_STYLES.Default;
}

function createGroupOptions() {
    return Object.entries(NODE_TYPE_STYLES).reduce((acc, [key, style]) => {
        acc[key] = {
            color: style.color,
            font: { color: style.fontColor },
        };
        return acc;
    }, {});
}

function isPlainObject(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
}

function truncate(text, max = 64) {
    if (typeof text !== "string") return "";
    return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function determineNodeLabel(node) {
    if (typeof node.display_name === "string" && node.display_name.trim()) {
        return truncate(node.display_name.trim());
    }

    if (isPlainObject(node.properties)) {
        const props = node.properties;
        const candidates = [
            "display_name",
            "name",
            "title",
            "slug",
            "identifier",
            "path",
            "file_path",
            "repo",
            "repository",
            "value",
        ];
        for (const key of candidates) {
            const value = props[key];
            if (typeof value === "string" && value.trim()) {
                return truncate(value.trim());
            }
        }
    }

    if (typeof node.id === "string") {
        return truncate(node.id);
    }

    return "未命名节点";
}

function buildNodeTooltip(node, label, nodeType) {
    const props = isPlainObject(node.properties) ? node.properties : {};
    const tooltipLines = [
        `类型: ${NODE_TYPE_STYLES[nodeType]?.displayName ?? nodeType}`,
        label && typeof node.id === "string" && label !== node.id ? `名称: ${label}` : null,
        props.file_path ? `路径: ${props.file_path}` : null,
        !props.file_path && props.path ? `路径: ${props.path}` : null,
        props.language ? `语言: ${props.language}` : null,
        props.revision ? `版本: ${props.revision}` : null,
        typeof node.id === "string" ? `ID: ${node.id}` : null,
    ];
    return tooltipLines.filter(Boolean).join("\n");
}

function renderLegend(typesInGraph) {
    if (!graphLegend) return;
    const uniqueTypes = Array.from(
        new Set(typesInGraph.map((type) => NODE_TYPE_STYLES[type]?.displayName ?? type)),
    );
    if (!uniqueTypes.length) {
        graphLegend.innerHTML = "";
        graphLegend.classList.add("hidden");
        return;
    }

    graphLegend.innerHTML = "";
    graphLegend.classList.remove("hidden");

    const usedTypes = Array.from(new Set(typesInGraph));
    usedTypes.forEach((type) => {
        const style = getNodeStyle(type);
        const element = document.createElement("div");
        element.className = "legend-item";

        const swatch = document.createElement("span");
        swatch.className = "legend-swatch";
        swatch.style.background = style.color.background;
        swatch.style.borderColor = style.color.border;

        const label = document.createElement("span");
        label.textContent = style.displayName;

        element.appendChild(swatch);
        element.appendChild(label);
        graphLegend.appendChild(element);
    });
}

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
    const centerId = graphJson.center?.id ?? null;
    const seenTypes = new Set();
    const nodes = (graphJson.nodes || []).map((node) => {
        const nodeType = normalizeType(node.entity_type);
        seenTypes.add(nodeType);
        const label = determineNodeLabel(node);
        const tooltip = buildNodeTooltip(node, label, nodeType);
        const style = getNodeStyle(nodeType);
        const dataset = {
            id: node.id,
            label,
            group: nodeType,
            title: tooltip,
            color: style.color,
            font: { color: style.fontColor },
        };
        if (centerId && node.id === centerId) {
            dataset.borderWidth = 3;
            dataset.size = 22;
        }
        return dataset;
    });

    const edges = (graphJson.edges || []).map((edge) => ({
        from: edge.from,
        to: edge.to,
        label: edge.label || "",
        title:
            isPlainObject(edge.properties) && Object.keys(edge.properties).length
                ? JSON.stringify(edge.properties, null, 2)
                : "",
    }));

    return { nodes, edges, types: Array.from(seenTypes) };
}

function renderGraph(graphJson) {
    const container = document.getElementById("graph-container");
    const { nodes, edges, types } = buildGraphData(graphJson);
    renderLegend(types);

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
            groups: createGroupOptions(),
        });
    } else {
        network.setOptions({ groups: createGroupOptions() });
        network.setData({ nodes: new vis.DataSet(nodes), edges: new vis.DataSet(edges) });
    }
}

function populateStartNodeSelect(candidates) {
    if (!graphStartSelect) return;
    graphStartSelect.innerHTML = "";
    graphCandidates = candidates;

    candidates.forEach((candidate, index) => {
        const option = document.createElement("option");
        option.value = candidate.id;
        const label =
            (candidate.display_name && candidate.display_name.trim()) || candidate.id;
        option.textContent = `${label} · ${candidate.entity_type}`;
        if (index === 0) option.selected = true;
        graphStartSelect.appendChild(option);
    });
}

async function loadGraphOverview() {
    graphError.textContent = "";
    try {
        const data = await fetchJSON("/api/graph/overview");
        const candidates = Array.isArray(data.candidates) ? data.candidates : [];
        if (!candidates.length) {
            graphError.textContent = "暂无图节点可展示。";
            renderGraph({ nodes: [], edges: [], center: null });
            return;
        }
        populateStartNodeSelect(candidates);
        const defaultId = graphStartSelect?.value;
        if (defaultId) {
            await loadSubgraph({ startId: defaultId, showError: false });
        }
    } catch (error) {
        graphError.textContent = `概览加载失败: ${error.message}`;
    }
}

async function loadSubgraph({ startId, showError = true } = {}) {
    graphError.textContent = "";
    const nodeId = startId || graphStartSelect?.value || "";
    if (!nodeId) {
        if (showError) {
            graphError.textContent = "请选择起始节点。";
        }
        return;
    }

    try {
        const depth = Number(graphDepthInput?.value) || 1;
        const nodeLimit = Number(graphNodeLimitInput?.value) || 150;
        const edgeTypes = graphEdgeTypesInput?.value.trim();

        const params = new URLSearchParams();
        params.set("start_id", nodeId);
        params.set("depth", Math.max(0, Math.min(depth, 4)));
        params.set("node_limit", Math.min(Math.max(nodeLimit, 1), 300));
        params.set("edge_limit", 300);
        if (edgeTypes) params.set("edge_types", edgeTypes);

        const graphJson = await fetchJSON(`/api/graph/subgraph?${params.toString()}`);
        renderGraph(graphJson);
    } catch (error) {
        if (showError) {
            graphError.textContent = `图数据加载失败: ${error.message}`;
        }
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
        loadGraphOverview();
        loadTables();
    });
    $("#refresh-status").addEventListener("click", loadStatus);
    $("#refresh-fetchers").addEventListener("click", loadFetchers);
    $("#refresh-graph").addEventListener("click", () => loadSubgraph({}));
    $("#refresh-tables").addEventListener("click", loadTables);
    $("#run-readiness").addEventListener("click", runReadiness);
    $("#run-sync").addEventListener("click", runSync);

    if (graphStartSelect) {
        graphStartSelect.addEventListener("change", () =>
            loadSubgraph({ startId: graphStartSelect.value }),
        );
    }
}

async function bootstrap() {
    attachEventHandlers();
    await Promise.all([loadStatus(), loadFetchers(), loadTables()]);
    await loadGraphOverview();
    if (syncOutput) {
        syncOutput.textContent = "等待执行...";
    }
}

document.addEventListener("DOMContentLoaded", bootstrap);
