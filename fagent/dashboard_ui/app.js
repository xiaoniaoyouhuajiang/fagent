const $ = (selector) => document.querySelector(selector);

const statusOutput = $("#status-output");
const fetcherList = $("#fetcher-list");
const tablesOutput = $("#tables-output");
const readinessOutput = $("#readiness-output");
const syncFetcherInput = $("#sync-fetcher");
const syncQueryInput = $("#sync-query");
const syncBudgetType = $("#sync-budget-type");
const syncBudgetValue = $("#sync-budget-value");
const syncParamsInput = $("#sync-params");
const syncTargetsInput = $("#sync-targets");
const syncOutput = $("#sync-output");
const hybridQueryInput = $("#hybrid-query");
const hybridEntitiesInput = $("#hybrid-entities");
const hybridAlphaInput = $("#hybrid-alpha");
const hybridLimitInput = $("#hybrid-limit");
const hybridOutput = $("#hybrid-output");

function isPlainObject(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
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

        if (fetcher.toLowerCase() === "gitfetcher") {
            const includeIssues = $("#include-issues");
            const includePulls = $("#include-pulls");
            const includeDevelopers = $("#include-developers");
            const docLevelOnly = $("#doc-level-only");
            const touchesMode = $("#touches-mode");
            const representativeLimit = $("#representative-limit");

            if (includeIssues) {
                params.include_issues = includeIssues.checked;
            }
            if (includePulls) {
                params.include_pulls = includePulls.checked;
            }
            if (includeDevelopers) {
                params.include_developers = includeDevelopers.checked;
            }
            if (docLevelOnly) {
                params.doc_level_only = docLevelOnly.checked;
            }
            if (touchesMode && touchesMode.value) {
                params.touches_mode = touchesMode.value;
            }
            if (representativeLimit) {
                const limitValue = Number(representativeLimit.value);
                if (!Number.isNaN(limitValue) && limitValue > 0) {
                    params.representative_comment_limit = Math.floor(limitValue);
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

async function loadHybridDefaults() {
    if (!hybridEntitiesInput) {
        return;
    }
    try {
        const types = await fetchJSON("/api/search/hybrid/types");
        if (types.length && !hybridEntitiesInput.value) {
            hybridEntitiesInput.value = types.join(", ");
        }
    } catch (error) {
        console.warn("加载混合检索实体列表失败:", error);
    }
}

async function runHybridSearch() {
    if (!hybridOutput) {
        return;
    }
    const query = (hybridQueryInput?.value || "").trim();
    const entityText = (hybridEntitiesInput?.value || "").trim();
    if (!query) {
        hybridOutput.textContent = "请输入查询文本。";
        return;
    }

    const alpha =
        Number(hybridAlphaInput?.value ?? 0.5) >= 0
            ? Number(hybridAlphaInput?.value ?? 0.5)
            : 0.5;
    const limit =
        Math.max(
            1,
            Math.min(200, Number(hybridLimitInput?.value ?? 20) || 20),
        ) || 20;

    const params = new URLSearchParams();
    params.set("q", query);
    params.set("alpha", alpha.toString());
    params.set("limit", limit.toString());
    if (entityText) {
        params.set("entity_types", entityText);
    }

    hybridOutput.textContent = "查询中…";
    try {
        const response = await fetchJSON(`/api/search/hybrid_all?${params.toString()}`);
        hybridOutput.textContent = JSON.stringify(response, null, 2);
    } catch (error) {
        hybridOutput.textContent = `查询失败: ${error.message}`;
    }
}

function attachEventHandlers() {
    $("#refresh-all").addEventListener("click", () => {
        loadStatus();
        loadFetchers();
        loadTables();
    });
    $("#refresh-status").addEventListener("click", loadStatus);
    $("#refresh-fetchers").addEventListener("click", loadFetchers);
    $("#refresh-tables").addEventListener("click", loadTables);
    $("#run-readiness").addEventListener("click", runReadiness);
    $("#run-sync").addEventListener("click", runSync);
    $("#run-hybrid-search")?.addEventListener("click", runHybridSearch);
}

async function bootstrap() {
    attachEventHandlers();
    await Promise.all([loadStatus(), loadFetchers(), loadTables(), loadHybridDefaults()]);
    if (syncOutput) {
        syncOutput.textContent = "等待执行...";
    }
    if (hybridOutput) {
        hybridOutput.textContent = "等待查询…";
    }
}

document.addEventListener("DOMContentLoaded", bootstrap);
