const $ = (selector) => document.querySelector(selector);

const searchInput = $("#graph-search-input");
const searchResults = $("#graph-search-results");
const selectedHint = $("#graph-selected");
const graphError = $("#graph-error");
const graphLegend = $("#graph-legend");
const graphDepthInput = $("#graph-depth");
const graphNodeLimitInput = $("#graph-node-limit");
const graphEdgeLimitInput = $("#graph-edge-limit");
const graphEdgeTypesInput = $("#graph-edge-types");
const loadButton = $("#graph-load");
const typeFilterSelect = $("#graph-type-filter");
const pathFromInput = $("#graph-path-from");
const pathToInput = $("#graph-path-to");
const pathEdgeInput = $("#graph-path-edge");
const pathRunButton = $("#graph-path-run");
const pathResultBox = $("#graph-path-result");

let network = null;
let graphCandidates = [];
let selectedNodeId = null;

const typeStyles = new Map();
const aliasLookup = new Map();
const fallbackPalette = [
    { background: "#0ea5e9", border: "#38bdf8" },
    { background: "#22d3ee", border: "#67e8f9" },
    { background: "#f97316", border: "#fb923c" },
    { background: "#6366f1", border: "#818cf8" },
    { background: "#8b5cf6", border: "#a855f7" },
    { background: "#14b8a6", border: "#2dd4bf" },
    { background: "#facc15", border: "#fde047" },
    { background: "#22c55e", border: "#4ade80" },
    { background: "#e879f9", border: "#f0abfc" },
    { background: "#f59e0b", border: "#fbbf24" },
];
let fallbackIndex = 0;
let typeStylesLoaded = false;

function hexToRgb(hex) {
    if (typeof hex !== "string") return null;
    const normalized = hex.replace("#", "").trim();
    if (normalized.length !== 6) return null;
    const bigint = Number.parseInt(normalized, 16);
    if (Number.isNaN(bigint)) return null;
    return {
        r: (bigint >> 16) & 255,
        g: (bigint >> 8) & 255,
        b: bigint & 255,
    };
}

function relativeLuminance(hex) {
    const rgb = hexToRgb(hex);
    if (!rgb) return null;
    const channels = [rgb.r, rgb.g, rgb.b].map((channel) => {
        const v = channel / 255;
        return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
}

function contrastRatio(foreground, background) {
    const fgLum = relativeLuminance(foreground);
    const bgLum = relativeLuminance(background);
    if (fgLum == null || bgLum == null) return 0;
    const lighter = Math.max(fgLum, bgLum);
    const darker = Math.min(fgLum, bgLum);
    return (lighter + 0.05) / (darker + 0.05);
}

function pickReadableText(background, preferred) {
    const fallbackLight = "#f8fafc";
    const fallbackDark = "#020617";
    const preferredContrast = preferred ? contrastRatio(preferred, background) : 0;
    const lightContrast = contrastRatio(fallbackLight, background);
    const darkContrast = contrastRatio(fallbackDark, background);
    const betterFallback = lightContrast >= darkContrast ? fallbackLight : fallbackDark;
    if (preferredContrast >= Math.max(3.5, betterFallback === fallbackLight ? lightContrast : darkContrast)) {
        return preferred;
    }
    return betterFallback;
}

function normalizeStylePayload(entry) {
    if (!entry) {
        return null;
    }
    const colors = entry.color || {};
    const background = colors.background || "#475569";
    const border = colors.border || background;
    const highlightBackground = colors.highlight_background || border;
    const highlightBorder = colors.highlight_border || border;
    const displayName = entry.display_name || entry.entity_type || "节点";
    const fontColor = pickReadableText(background, entry.font_color);
    return {
        entityType: entry.entity_type || "Default",
        displayName,
        fontColor,
        colors: {
            background,
            border,
            highlightBackground,
            highlightBorder,
        },
        aliases: Array.isArray(entry.aliases) ? entry.aliases : [],
    };
}

function registerTypeStyle(entry) {
    const normalized = normalizeStylePayload(entry);
    if (!normalized) return;
    typeStyles.set(normalized.entityType, normalized);
    aliasLookup.set(normalized.entityType.toUpperCase(), normalized.entityType);
    normalized.aliases.forEach((alias) => {
        if (typeof alias === "string" && alias.trim()) {
            aliasLookup.set(alias.trim().toUpperCase(), normalized.entityType);
        }
    });
}

function ensureFallbackStyle(rawType) {
    const key = rawType && rawType.trim() ? rawType.trim() : "Default";
    if (typeStyles.has(key)) return key;
    const palette = fallbackPalette[fallbackIndex % fallbackPalette.length];
    fallbackIndex += 1;
    const background = palette.background;
    const border = palette.border || background;
    const highlightBackground = palette.highlightBackground || border;
    const highlightBorder = palette.highlightBorder || border;
    const fontColor = pickReadableText(background);
    typeStyles.set(key, {
        entityType: key,
        displayName: key,
        fontColor,
        colors: {
            background,
            border,
            highlightBackground,
            highlightBorder,
        },
        aliases: [],
    });
    aliasLookup.set(key.toUpperCase(), key);
    return key;
}

function resolveTypeKey(rawType) {
    if (!rawType) {
        return ensureFallbackStyle("Default");
    }
    const trimmed = String(rawType).trim();
    if (!trimmed) {
        return ensureFallbackStyle("Default");
    }
    if (typeStyles.has(trimmed)) {
        return trimmed;
    }
    const lookup = aliasLookup.get(trimmed.toUpperCase());
    if (lookup) {
        return lookup;
    }
    return ensureFallbackStyle(trimmed);
}

function getTypeStyle(key) {
    if (!key) return typeStyles.get("Default");
    if (typeStyles.has(key)) return typeStyles.get(key);
    const resolved = resolveTypeKey(key);
    return typeStyles.get(resolved);
}

function ensureTypeStyle(rawType) {
    const key = resolveTypeKey(rawType);
    const style = getTypeStyle(key);
    return { key, style };
}

function createGroupOptions() {
    const groups = {};
    typeStyles.forEach((style, key) => {
        groups[key] = {
            color: {
                background: style.colors.background,
                border: style.colors.border,
                highlight: {
                    background: style.colors.highlightBackground,
                    border: style.colors.highlightBorder,
                },
                hover: {
                    background: style.colors.highlightBackground,
                    border: style.colors.highlightBorder,
                },
            },
            font: { color: style.fontColor },
        };
    });
    return groups;
}

function updateLegend(usedTypes = null) {
    if (!graphLegend) return;
    const keys = usedTypes ? Array.from(usedTypes) : Array.from(typeStyles.keys());
    const legendHtml = keys
        .filter((key) => key && key !== "Default")
        .map((key) => {
            const style = typeStyles.get(key);
            if (!style) return null;
            const swatch = `
                <span
                    class="legend-item"
                    style="background:${style.colors.background};border-color:${style.colors.border};color:${style.fontColor};"
                >
                    <i class="legend-swatch" style="background:${style.colors.border};"></i>
                    ${style.displayName}
                </span>
            `;
            return swatch;
        })
        .filter(Boolean)
        .join("");
    graphLegend.innerHTML = legendHtml;
    if (legendHtml) {
        graphLegend.classList.remove("hidden");
    } else {
        graphLegend.classList.add("hidden");
    }
}

function renderPathSummary(result) {
    if (!result || !Array.isArray(result.nodes)) {
        return "";
    }
    const headline = `<p>路径长度：${Number.isFinite(result.length) ? result.length : 0}</p>`;
    const nodeItems = result.nodes
        .map((node, index) => {
            const label =
                (node.display_name && node.display_name.trim()) ||
                (node.properties?.name && String(node.properties.name)) ||
                node.id;
            const entity = node.entity_type || "节点";
            return `<li>${index + 1}. ${label} <small>(${entity})</small></li>`;
        })
        .join("");
    return `<div class="path-summary">${headline}<ol>${nodeItems}</ol></div>`;
}

async function runShortestPath() {
    if (!pathFromInput || !pathToInput || !pathResultBox) {
        return;
    }
    const fromId = pathFromInput.value.trim();
    const toId = pathToInput.value.trim();
    const edgeLabel = pathEdgeInput?.value.trim() || "";
    if (!fromId || !toId) {
        pathResultBox.textContent = "请填写起点与终点节点的 UUID。";
        return;
    }
    pathResultBox.textContent = "正在计算最短路径…";
    try {
        const params = new URLSearchParams();
        params.set("from_id", fromId);
        params.set("to_id", toId);
        if (edgeLabel) {
            params.set("edge_label", edgeLabel);
        }
        const result = await fetchJSON(`/api/graph/shortest_path?${params.toString()}`);
        if (!result || !result.found) {
            pathResultBox.textContent = "未找到满足条件的路径。";
            return;
        }
        pathResultBox.innerHTML = renderPathSummary(result);
        renderGraph({ nodes: result.nodes || [], edges: result.edges || [] });
    } catch (error) {
        pathResultBox.textContent = `路径搜索失败: ${error.message}`;
    }
}

async function loadTypeStyles() {
    if (typeStylesLoaded) return;
    try {
        const data = await fetchJSON("/api/graph/types");
        if (Array.isArray(data)) {
            data.forEach((entry) => registerTypeStyle(entry));
        }
    } catch (error) {
        console.warn("Failed to load graph type styles, falling back to defaults:", error);
    } finally {
        if (!typeStyles.has("Default")) {
            registerTypeStyle({
                entity_type: "Default",
                display_name: "其他",
                font_color: "#e2e8f0",
                color: {
                    background: "#475569",
                    border: "#94a3b8",
                    highlight_background: "#94a3b8",
                    highlight_border: "#475569",
                },
            });
        }
        typeStylesLoaded = true;
    }
}

function populateTypeFilterOptions() {
    if (!typeFilterSelect || !typeStyles.size) return;
    const current = typeFilterSelect.value;
    const options = Array.from(typeStyles.values())
        .filter((style) => style.entityType && style.entityType !== "Default")
        .sort((a, b) => a.displayName.localeCompare(b.displayName, "zh-Hans-CN"));
    const fragment = document.createDocumentFragment();
    const defaultOption = document.createElement("option");
    defaultOption.value = "";
    defaultOption.textContent = "全部类型";
    fragment.appendChild(defaultOption);
    options.forEach((style) => {
        const option = document.createElement("option");
        option.value = style.entityType;
        option.textContent = style.displayName;
        fragment.appendChild(option);
    });
    typeFilterSelect.innerHTML = "";
    typeFilterSelect.appendChild(fragment);
    if (current && typeStyles.has(current)) {
        typeFilterSelect.value = current;
    }
}

function getSelectedTypeFilter() {
    if (!typeFilterSelect) return "";
    const value = typeFilterSelect.value;
    return value && value.trim() ? value.trim() : "";
}

function renderGraph(data) {
    if (!data || !Array.isArray(data.nodes)) {
        graphError.textContent = "无法渲染图数据。";
        return;
    }

    const usedTypes = new Set();
    const nodes = data.nodes.map((node) => {
        const { key: group, style } = ensureTypeStyle(node.entity_type || node.label);
        if (group) {
            usedTypes.add(group);
        }
        const label =
            (node.display_name && node.display_name.trim()) ||
            (node.properties?.name && String(node.properties.name)) ||
            node.id;
        const color = style
            ? {
                  background: style.colors.background,
                  border: style.colors.border,
                  highlight: {
                      background: style.colors.highlightBackground,
                      border: style.colors.highlightBorder,
                  },
                  hover: {
                      background: style.colors.highlightBackground,
                      border: style.colors.highlightBorder,
                  },
              }
            : undefined;
        const font = style ? { color: style.fontColor } : undefined;
        return {
            id: node.id,
            label,
            group,
            title: `${label}\n${node.entity_type || ""}`,
            color,
            font,
        };
    });
    updateLegend(usedTypes);

    const edges = (data.edges || []).map((edge) => ({
        id: edge.id,
        from: edge.from,
        to: edge.to,
        label: edge.label,
    }));

    const container = $("#graph-container");
    if (!container) {
        return;
    }

    if (!network) {
        network = new vis.Network(
            container,
            { nodes, edges },
            {
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
            },
        );
    } else {
        network.setOptions({ groups: createGroupOptions() });
        network.setData({
            nodes: new vis.DataSet(nodes),
            edges: new vis.DataSet(edges),
        });
    }
}

async function fetchJSON(url, options) {
    const response = await fetch(url, options);
    if (!response.ok) {
        const message = await response.text();
        throw new Error(message || response.statusText);
    }
    return response.json();
}

function debounce(fn, delay = 250) {
    let timer;
    return (...args) => {
        clearTimeout(timer);
        timer = setTimeout(() => fn(...args), delay);
    };
}

function updateSelectedHint(candidate) {
    if (!selectedHint) return;
    if (!candidate) {
        selectedHint.textContent = "尚未选择节点";
    } else {
        const label = candidate.display_name || candidate.id;
        selectedHint.textContent = `已选择：${label}（${candidate.entity_type}）`;
    }
}

function renderSuggestions(candidates) {
    graphCandidates = candidates;
    if (!searchResults) return;
    searchResults.innerHTML = "";
    if (!candidates.length) {
        searchResults.classList.add("hidden");
        return;
    }

    candidates.forEach((candidate) => {
        const item = document.createElement("button");
        item.type = "button";
        item.className = "suggestion-item";
        const label =
            (candidate.display_name && candidate.display_name.trim()) || candidate.id;
        item.textContent = `${label} · ${candidate.entity_type}`;
        item.title = candidate.id;
        item.addEventListener("click", () => {
            selectCandidate(candidate, { autoLoad: true });
        });
        searchResults.appendChild(item);
    });

    searchResults.classList.remove("hidden");
}

function selectCandidate(candidate, { autoLoad = false } = {}) {
    const label =
        (candidate.display_name && candidate.display_name.trim()) || candidate.id;
    if (searchInput) {
        searchInput.value = label;
    }
    selectedNodeId = candidate.id;
    updateSelectedHint(candidate);
    renderSuggestions([]);
    if (autoLoad) {
        loadSubgraph({ startId: candidate.id, showError: false });
    }
}

async function performSearch(term) {
    try {
        const params = new URLSearchParams();
        params.set("limit", "20");
        if (term && term.trim()) {
            params.set("q", term.trim());
        }
        const typeFilter = getSelectedTypeFilter();
        if (typeFilter) {
            params.set("entity_type", typeFilter);
        }
        const data = await fetchJSON(`/api/graph/search?${params.toString()}`);
        const candidates = Array.isArray(data.candidates) ? data.candidates : [];
        renderSuggestions(candidates);
        if (!selectedNodeId && candidates.length) {
            updateSelectedHint(candidates[0]);
        } else if (!selectedNodeId) {
            updateSelectedHint(null);
        }
    } catch (error) {
        graphError.textContent = `搜索失败: ${error.message}`;
    }
}

const debouncedSearch = debounce((term) => {
    if (!term || term.trim().length < 2) {
        performSearch("");
        return;
    }
    performSearch(term);
}, 250);

async function loadSubgraph({ startId, showError = true } = {}) {
    graphError.textContent = "";
    if (pathResultBox) {
        pathResultBox.textContent = "";
    }
    const nodeId = startId || selectedNodeId || (searchInput?.value || "").trim();
    if (!nodeId) {
        if (showError) {
            graphError.textContent = "请先选择或输入起始节点。";
        }
        return;
    }

    try {
        const depth = Number(graphDepthInput?.value) || 1;
        const nodeLimit = Number(graphNodeLimitInput?.value) || 150;
        const edgeLimit = Number(graphEdgeLimitInput?.value) || 300;
        const edgeTypes = graphEdgeTypesInput?.value.trim();

        const params = new URLSearchParams();
        params.set("start_id", nodeId);
        params.set("depth", Math.max(0, Math.min(depth, 4)));
        params.set("node_limit", Math.min(Math.max(nodeLimit, 1), 300));
        params.set("edge_limit", Math.min(Math.max(edgeLimit, 10), 1000));
        if (edgeTypes) params.set("edge_types", edgeTypes);

        const graphJson = await fetchJSON(`/api/graph/subgraph?${params.toString()}`);
        renderGraph(graphJson);
        selectedNodeId = nodeId;
        const matched =
            graphCandidates.find((candidate) => candidate.id === nodeId) || null;
        updateSelectedHint(matched || { id: nodeId, entity_type: graphJson.center?.entity_type });
    } catch (error) {
        if (showError) {
            graphError.textContent = `图数据加载失败: ${error.message}`;
        }
    }
}

async function loadInitialSuggestions() {
    await performSearch("");
    if (graphCandidates.length) {
        selectCandidate(graphCandidates[0], { autoLoad: true });
    } else {
        updateSelectedHint(null);
    }
}

function attachEventHandlers() {
    if (searchInput) {
        searchInput.addEventListener("input", (event) => {
            const term = event.target.value;
            selectedNodeId = null;
            debouncedSearch(term);
        });
        searchInput.addEventListener("focus", () => {
            if (graphCandidates.length) {
                searchResults.classList.remove("hidden");
            }
        });
    }

    if (loadButton) {
        loadButton.addEventListener("click", () => loadSubgraph({ showError: true }));
    }

    if (pathRunButton) {
        pathRunButton.addEventListener("click", () => runShortestPath());
    }

    [pathFromInput, pathToInput, pathEdgeInput].forEach((input) => {
        input?.addEventListener("keydown", (event) => {
            if (event.key === "Enter") {
                event.preventDefault();
                runShortestPath();
            }
        });
    });

    if (typeFilterSelect) {
        typeFilterSelect.addEventListener("change", () => {
            selectedNodeId = null;
            updateSelectedHint(null);
            performSearch(searchInput?.value || "");
        });
    }

    document.addEventListener("click", (event) => {
        if (!searchResults || !searchInput) return;
        if (
            !searchResults.contains(event.target) &&
            event.target !== searchInput
        ) {
            searchResults.classList.add("hidden");
        }
    });
}

async function bootstrap() {
    attachEventHandlers();
    await loadTypeStyles();
    populateTypeFilterOptions();
    updateLegend();
    await loadInitialSuggestions();
}

document.addEventListener("DOMContentLoaded", bootstrap);
