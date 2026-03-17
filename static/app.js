const runtimeQuery = new URLSearchParams(window.location.search);
const runtimeAndroid = runtimeQuery.get("android") === "1" || /Android/i.test(navigator.userAgent || "");
const FAVORITES_KEY = "bf_favorites_v2";
const PREFS_KEY = "bf_prefs_v3";
const DEFAULT_THEME_OPTIONS = [
  { id: "dark_ref", name: "Dark Ref" },
  { id: "pink_cyber", name: "Pink Cyber Pastel" },
  { id: "old_neko", name: "Old Neko" },
  { id: "retro_blue", name: "Neo 2005 CRT" },
];

const state = {
  platform: runtimeAndroid ? "android" : "desktop",
  view: "home",
  searchMode: "builder",
  builderMode: "include",
  includeTags: [],
  excludeTags: [],
  pageMap: new Map(),
  pageHasMore: new Map(),
  currentPage: 0,
  searchMeta: null,
  loading: false,
  sessionId: 0,
  autocompleteItems: [],
  autocompleteAbort: null,
  autocompleteReqId: 0,
  autocompleteCache: new Map(),
  home: null,
  trendingWindow: "7d",
  favorites: new Map(),
  viewerItems: [],
  viewerIndex: -1,
  secureSaveTimer: null,
  themeOptions: DEFAULT_THEME_OPTIONS.slice(),
  downloadBusy: false,
};

const dom = {
  workspace: document.getElementById("workspace"),
  topNav: document.getElementById("topNav"),
  mobileNav: document.getElementById("mobileNav"),
  builderTabBtn: document.getElementById("builderTabBtn"),
  advancedTabBtn: document.getElementById("advancedTabBtn"),
  builderBox: document.getElementById("builderBox"),
  advancedBox: document.getElementById("advancedBox"),
  builderModeBtn: document.getElementById("builderModeBtn"),
  builderInput: document.getElementById("builderInput"),
  addTagBtn: document.getElementById("addTagBtn"),
  autocompleteMenu: document.getElementById("autocompleteMenu"),
  selectedTags: document.getElementById("selectedTags"),
  rawQueryInput: document.getElementById("rawQueryInput"),
  sourceSelect: document.getElementById("sourceSelect"),
  sortSelect: document.getElementById("sortSelect"),
  ratingSelect: document.getElementById("ratingSelect"),
  minScoreSelect: document.getElementById("minScoreSelect"),
  limitSelect: document.getElementById("limitSelect"),
  adultCheckbox: document.getElementById("adultCheckbox"),
  searchBtn: document.getElementById("searchBtn"),
  clearQueryBtn: document.getElementById("clearQueryBtn"),
  loadMoreBtn: document.getElementById("loadMoreBtn"),
  favoritesBtn: document.getElementById("favoritesBtn"),
  openDownloadsBtn: document.getElementById("openDownloadsBtn"),
  openSettingsBtn: document.getElementById("openSettingsBtn"),
  resolvedQueryBar: document.getElementById("resolvedQueryBar"),
  homeView: document.getElementById("homeView"),
  resultsView: document.getElementById("resultsView"),
  favoritesView: document.getElementById("favoritesView"),
  settingsView: document.getElementById("settingsView"),
  homeStatus: document.getElementById("homeStatus"),
  recentQueries: document.getElementById("recentQueries"),
  trendingTabs: document.getElementById("trendingTabs"),
  trendingSections: document.getElementById("trendingSections"),
  featuredSections: document.getElementById("featuredSections"),
  newsList: document.getElementById("newsList"),
  resultCount: document.getElementById("resultCount"),
  statusText: document.getElementById("statusText"),
  errorBox: document.getElementById("errorBox"),
  activeQuery: document.getElementById("activeQuery"),
  grid: document.getElementById("grid"),
  paginationBar: document.getElementById("paginationBar"),
  favoritesTitle: document.getElementById("favoritesTitle"),
  favoritesGrid: document.getElementById("favoritesGrid"),
  securityStatus: document.getElementById("securityStatus"),
  rule34UserId: document.getElementById("rule34UserId"),
  rule34ApiKey: document.getElementById("rule34ApiKey"),
  gelbooruUserId: document.getElementById("gelbooruUserId"),
  gelbooruApiKey: document.getElementById("gelbooruApiKey"),
  proxyUrl: document.getElementById("proxyUrl"),
  themeSelect: document.getElementById("themeSelect"),
  refreshHomeBtn: document.getElementById("refreshHomeBtn"),
  clearSecureBtn: document.getElementById("clearSecureBtn"),
  viewer: document.getElementById("viewer"),
  mediaHolder: document.getElementById("mediaHolder"),
  metaBox: document.getElementById("metaBox"),
  downloadCurrentBtn: document.getElementById("downloadCurrentBtn"),
  likeCurrentBtn: document.getElementById("likeCurrentBtn"),
  prevBtn: document.getElementById("prevBtn"),
  nextBtn: document.getElementById("nextBtn"),
  closeBtn: document.getElementById("closeBtn"),
};

document.body.dataset.platform = state.platform;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function normalizePhrase(value) {
  return String(value || "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function dedupe(values) {
  return [...new Set((values || []).map((value) => normalizePhrase(value)).filter(Boolean))];
}

function formatNumber(value) {
  return new Intl.NumberFormat("ru-RU").format(Number(value || 0));
}

function isVideoUrl(url) {
  const value = String(url || "").toLowerCase();
  return [".mp4", ".webm", ".mov", ".m4v", ".avi", ".mkv"].some((ext) => value.includes(ext));
}

function proxyMediaUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  return `/api/media?url=${encodeURIComponent(raw)}`;
}

function pickThumb(item) {
  const preview = String(item.preview_url || "");
  const sample = String(item.sample_url || "");
  const file = String(item.file_url || "");
  const still = [sample, preview, file].find((url) => url && !isVideoUrl(url)) || "";
  if (item.media_type === "video") {
    return { kind: still ? "image" : "video", src: still || sample || file };
  }
  return { kind: "image", src: sample || preview || file };
}

function selectedSources() {
  const value = String(dom.sourceSelect.value || "both").trim();
  return value === "both" ? ["rule34", "gelbooru"] : [value];
}

function buildAuthHeaders() {
  return {
    "X-Rule34-User-Id": dom.rule34UserId.value.trim(),
    "X-Rule34-Api-Key": dom.rule34ApiKey.value.trim(),
    "X-Gelbooru-User-Id": dom.gelbooruUserId.value.trim(),
    "X-Gelbooru-Api-Key": dom.gelbooruApiKey.value.trim(),
    "X-Booru-Proxy-Url": dom.proxyUrl.value.trim(),
  };
}

function setStatus(text) {
  dom.statusText.textContent = text || "Ready";
}

function setError(text) {
  if (!text) {
    dom.errorBox.classList.add("hidden");
    dom.errorBox.textContent = "";
    return;
  }
  dom.errorBox.textContent = text;
  dom.errorBox.classList.remove("hidden");
}

function favoriteKey(item) {
  return `${item.source_id}:${item.id}`;
}

function isFavorited(item) {
  return item ? state.favorites.has(favoriteKey(item)) : false;
}

function favoriteList() {
  return [...state.favorites.values()].sort((a, b) => Number(b.favorite_ts || 0) - Number(a.favorite_ts || 0));
}

function saveFavorites() {
  localStorage.setItem(FAVORITES_KEY, JSON.stringify(favoriteList()));
}

function loadFavorites() {
  state.favorites.clear();
  try {
    const raw = localStorage.getItem(FAVORITES_KEY);
    if (!raw) return;
    const items = JSON.parse(raw);
    if (!Array.isArray(items)) return;
    items.forEach((item) => {
      if (item?.source_id && item?.id) {
        state.favorites.set(favoriteKey(item), item);
      }
    });
  } catch {
    // no-op
  }
}
function collectPrefs() {
  return {
    theme: document.body.dataset.theme || "dark_ref",
    source: dom.sourceSelect.value || "both",
    sort: dom.sortSelect.value || "popular",
    rating: dom.ratingSelect.value || "any",
    minScore: dom.minScoreSelect.value || "0",
    limit: dom.limitSelect.value || "36",
    adult: dom.adultCheckbox.checked,
    searchMode: state.searchMode,
  };
}

function savePrefs() {
  localStorage.setItem(PREFS_KEY, JSON.stringify(collectPrefs()));
}

function loadPrefs() {
  try {
    const raw = localStorage.getItem(PREFS_KEY);
    if (!raw) return;
    const prefs = JSON.parse(raw);
    if (prefs.theme) applyTheme(prefs.theme);
    if (prefs.source) dom.sourceSelect.value = prefs.source;
    if (prefs.sort) dom.sortSelect.value = prefs.sort;
    if (prefs.rating) dom.ratingSelect.value = prefs.rating;
    if (prefs.minScore) dom.minScoreSelect.value = prefs.minScore;
    if (prefs.limit) dom.limitSelect.value = prefs.limit;
    dom.adultCheckbox.checked = Boolean(prefs.adult);
    if (prefs.searchMode === "advanced") setSearchMode("advanced");
  } catch {
    // no-op
  }
}

function applyTheme(themeId) {
  const nextTheme = state.themeOptions.some((item) => item.id === themeId) ? themeId : "dark_ref";
  document.body.dataset.theme = nextTheme;
  if (dom.themeSelect.value !== nextTheme) {
    dom.themeSelect.value = nextTheme;
  }
  savePrefs();
}

function syncThemeOptions() {
  dom.themeSelect.innerHTML = state.themeOptions
    .map((item) => `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)}</option>`)
    .join("");
  const desired = document.body.dataset.theme || "dark_ref";
  dom.themeSelect.value = state.themeOptions.some((item) => item.id === desired) ? desired : state.themeOptions[0].id;
}

function updateFavoritesButton() {
  dom.favoritesBtn.textContent = `Favorites (${state.favorites.size})`;
  dom.favoritesBtn.classList.toggle("toggle-active", state.view === "favorites");
}

function toggleFavorite(item) {
  if (!item) return;
  const key = favoriteKey(item);
  if (state.favorites.has(key)) {
    state.favorites.delete(key);
    setStatus("Removed from favorites");
  } else {
    state.favorites.set(key, { ...item, favorite_ts: Date.now() });
    setStatus("Saved to favorites");
  }
  saveFavorites();
  updateFavoritesButton();
  renderFavorites();
  renderResults();
  updateViewerLikeButton();
}

function setView(view) {
  state.view = view;
  dom.homeView.classList.toggle("view-active", view === "home");
  dom.homeView.classList.toggle("hidden", view !== "home");
  dom.resultsView.classList.toggle("view-active", view === "search");
  dom.resultsView.classList.toggle("hidden", view !== "search");
  dom.favoritesView.classList.toggle("view-active", view === "favorites");
  dom.favoritesView.classList.toggle("hidden", view !== "favorites");
  dom.settingsView.classList.toggle("view-active", view === "settings");
  dom.settingsView.classList.toggle("hidden", view !== "settings");
  dom.workspace.className = `workspace ${view}-mode`;
  document.querySelectorAll(".nav-btn, .mobile-nav-btn").forEach((button) => {
    button.classList.toggle("active", button.dataset.view === view);
  });
  updateFavoritesButton();
}

function setSearchMode(mode) {
  state.searchMode = mode;
  dom.builderBox.classList.toggle("hidden", mode !== "builder");
  dom.advancedBox.classList.toggle("hidden", mode !== "advanced");
  dom.builderTabBtn.classList.toggle("active", mode === "builder");
  dom.advancedTabBtn.classList.toggle("active", mode === "advanced");
  savePrefs();
}

function setBuilderMode(mode) {
  state.builderMode = mode === "exclude" ? "exclude" : "include";
  dom.builderModeBtn.textContent = state.builderMode === "include" ? "Include" : "Exclude";
  dom.builderModeBtn.classList.toggle("active", state.builderMode === "exclude");
}

function collectSecurePayload() {
  return {
    credentials: {
      rule34: { user_id: dom.rule34UserId.value.trim(), api_key: dom.rule34ApiKey.value.trim() },
      gelbooru: { user_id: dom.gelbooruUserId.value.trim(), api_key: dom.gelbooruApiKey.value.trim() },
    },
    network: { proxy_url: dom.proxyUrl.value.trim() },
  };
}

function applySecurePayload(payload) {
  const credentials = payload?.credentials || {};
  const network = payload?.network || {};
  dom.rule34UserId.value = credentials.rule34?.user_id || "";
  dom.rule34ApiKey.value = credentials.rule34?.api_key || "";
  dom.gelbooruUserId.value = credentials.gelbooru?.user_id || "";
  dom.gelbooruApiKey.value = credentials.gelbooru?.api_key || "";
  dom.proxyUrl.value = network.proxy_url || "";
}

async function refreshSecurityStatus() {
  try {
    const res = await fetch("/api/security");
    const data = await res.json();
    dom.securityStatus.textContent = `Security: ${String(data.storage || "plain").toUpperCase()} / HTTPS only / cache ${data.cache_db ? "ready" : "off"}`;
  } catch {
    dom.securityStatus.textContent = "Security: unavailable";
  }
}

async function loadSecureConfig() {
  try {
    const res = await fetch("/api/secure-config");
    const data = await res.json();
    if (res.ok) applySecurePayload(data);
  } catch {
    // no-op
  }
}

async function saveSecureConfig() {
  try {
    await fetch("/api/secure-config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(collectSecurePayload()),
    });
    refreshSecurityStatus();
  } catch {
    // no-op
  }
}

function queueSecureSave() {
  clearTimeout(state.secureSaveTimer);
  state.secureSaveTimer = setTimeout(saveSecureConfig, 320);
}

async function openDownloadsFolder() {
  try {
    const res = await fetch("/api/downloads/open", { method: "POST" });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error(data.error || "Cannot open downloads folder");
    }
  } catch (error) {
    setError(error.message || String(error));
  }
}

async function downloadItem(item) {
  if (!item?.file_url || state.downloadBusy) return;
  state.downloadBusy = true;
  setStatus(`Downloading ${item.id}...`);
  try {
    const res = await fetch("/api/download", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...buildAuthHeaders() },
      body: JSON.stringify({ url: item.file_url, source_id: item.source_id, post_id: item.id }),
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error(data.error || "Download failed");
    }
    setStatus(`Downloaded ${data.filename}`);
  } catch (error) {
    setError(error.message || String(error));
  } finally {
    state.downloadBusy = false;
  }
}

async function fetchSources() {
  const res = await fetch("/api/sources");
  const data = await res.json();
  const options = (data.sources || []).map((source) => `<option value="${escapeHtml(source.id)}">${escapeHtml(source.name)}</option>`);
  options.push('<option value="both">Rule34 + Gelbooru</option>');
  dom.sourceSelect.innerHTML = options.join("");
  state.themeOptions = Array.isArray(data.theme_options) && data.theme_options.length ? data.theme_options : DEFAULT_THEME_OPTIONS.slice();
  syncThemeOptions();
}
function removeTag(mode, index) {
  if (mode === "exclude") {
    state.excludeTags.splice(index, 1);
  } else {
    state.includeTags.splice(index, 1);
  }
  renderSelectedTags();
}

function addBuilderTag(rawValue, mode = state.builderMode) {
  const value = normalizePhrase(rawValue);
  if (!value) return false;
  if (mode === "exclude") {
    state.excludeTags = dedupe([...state.excludeTags, value]);
    state.includeTags = state.includeTags.filter((tag) => tag !== value);
  } else {
    state.includeTags = dedupe([...state.includeTags, value]);
    state.excludeTags = state.excludeTags.filter((tag) => tag !== value);
  }
  dom.builderInput.value = "";
  hideAutocomplete();
  renderSelectedTags();
  return true;
}

function renderSelectedTags() {
  const items = [];
  state.includeTags.forEach((tag, index) => {
    items.push(`<span class="tag-chip"><span>${escapeHtml(tag)}</span><button data-remove-tag="include:${index}" type="button">×</button></span>`);
  });
  state.excludeTags.forEach((tag, index) => {
    items.push(`<span class="tag-chip exclude"><span>${escapeHtml(tag)}</span><button data-remove-tag="exclude:${index}" type="button">×</button></span>`);
  });
  dom.selectedTags.innerHTML = items.join("") || '<span class="module-meta">No selected tags yet. Use Include / Exclude and add phrases from autocomplete.</span>';
}

function hideAutocomplete() {
  state.autocompleteItems = [];
  dom.autocompleteMenu.innerHTML = "";
  dom.autocompleteMenu.classList.add("hidden");
  state.autocompleteAbort?.abort();
  state.autocompleteAbort = null;
}

function renderAutocomplete(items) {
  state.autocompleteItems = items;
  if (!items.length) {
    hideAutocomplete();
    return;
  }
  dom.autocompleteMenu.innerHTML = items
    .map(
      (item, index) => `
        <button class="auto-item ${index === 0 ? "active" : ""}" data-auto-index="${index}" type="button">
          <span class="auto-main">
            <strong>${escapeHtml(item.label || item.value)}</strong>
            <span class="auto-site">${escapeHtml(item.source_name || "Source")}</span>
          </span>
          <span class="auto-count">${formatNumber(item.count)}</span>
        </button>
      `
    )
    .join("");
  dom.autocompleteMenu.classList.remove("hidden");
}

async function fetchAutocomplete() {
  if (state.searchMode !== "builder") {
    hideAutocomplete();
    return;
  }
  const term = normalizePhrase(dom.builderInput.value);
  if (term.length < 2 || !dom.adultCheckbox.checked) {
    hideAutocomplete();
    return;
  }
  const cacheKey = `${selectedSources().join(",")}|${term.toLowerCase()}`;
  if (state.autocompleteCache.has(cacheKey)) {
    renderAutocomplete(state.autocompleteCache.get(cacheKey));
    return;
  }
  state.autocompleteReqId += 1;
  const requestId = state.autocompleteReqId;
  state.autocompleteAbort?.abort();
  state.autocompleteAbort = new AbortController();
  const params = new URLSearchParams({ term, adult: "1", limit: "12", sources: selectedSources().join(",") });
  try {
    const res = await fetch(`/api/autocomplete?${params.toString()}`, {
      headers: buildAuthHeaders(),
      signal: state.autocompleteAbort.signal,
    });
    const data = await res.json();
    if (!res.ok || requestId !== state.autocompleteReqId) {
      throw new Error(data.error || "Autocomplete failed");
    }
    const items = Array.isArray(data.items) ? data.items : [];
    state.autocompleteCache.set(cacheKey, items);
    renderAutocomplete(items);
  } catch (error) {
    if (error?.name !== "AbortError") {
      hideAutocomplete();
    }
  }
}

function queueAutocomplete() {
  clearTimeout(queueAutocomplete.timer);
  queueAutocomplete.timer = setTimeout(fetchAutocomplete, 80);
}

function applyAutocompleteItem(index) {
  const item = state.autocompleteItems[index];
  if (!item) return;
  addBuilderTag(item.value, state.builderMode);
}

function commitPendingBuilderInput() {
  if (state.searchMode !== "builder") return false;
  const pending = normalizePhrase(dom.builderInput.value);
  if (!pending) return false;
  return addBuilderTag(pending, state.builderMode);
}

function resetSearchState() {
  state.pageMap.clear();
  state.pageHasMore.clear();
  state.currentPage = 0;
  state.searchMeta = null;
  dom.grid.innerHTML = "";
  dom.paginationBar.innerHTML = "";
  dom.activeQuery.innerHTML = "";
  renderResolvedQuery();
}

function clearQuery() {
  state.includeTags = [];
  state.excludeTags = [];
  dom.builderInput.value = "";
  dom.rawQueryInput.value = "";
  renderSelectedTags();
  resetSearchState();
  setStatus("Ready");
  setError("");
}

function populateQueryFromHistory(entry) {
  state.includeTags = dedupe(entry.include_tags || []);
  state.excludeTags = dedupe(entry.exclude_tags || []);
  dom.rawQueryInput.value = entry.raw_query || "";
  setSearchMode(entry.raw_query ? "advanced" : "builder");
  renderSelectedTags();
  if (Array.isArray(entry.sources) && entry.sources.length) {
    const joined = entry.sources.length === 2 ? "both" : entry.sources[0];
    if ([...dom.sourceSelect.options].some((option) => option.value === joined)) {
      dom.sourceSelect.value = joined;
    }
  }
  if (entry.sort_mode) dom.sortSelect.value = entry.sort_mode;
  if (entry.rating) dom.ratingSelect.value = entry.rating;
  if (entry.min_score != null) dom.minScoreSelect.value = String(entry.min_score);
  savePrefs();
}

function renderRecentQueries(items) {
  if (!items?.length) {
    dom.recentQueries.innerHTML = '<span class="module-meta">Recent searches will appear here after your first successful query.</span>';
    return;
  }
  dom.recentQueries.innerHTML = items
    .map(
      (item, index) => `
        <button class="recent-pill" data-recent-index="${index}" type="button">
          <span>${escapeHtml(item.display_query || "search")}</span>
          <small>${escapeHtml((item.sources || []).join(" + ") || "both")}</small>
        </button>
      `
    )
    .join("");
}

function renderTrendingTabs(activeWindow) {
  const windows = state.home?.trending?.windows || ["7d", "30d", "90d", "180d"];
  dom.trendingTabs.innerHTML = windows
    .map((windowKey) => `<button class="tab-btn ${windowKey === activeWindow ? "active" : ""}" data-window="${windowKey}" type="button">${escapeHtml(windowKey)}</button>`)
    .join("");
}

function renderHomeSections(target, sections, type) {
  if (!sections?.length) {
    target.innerHTML = '<span class="module-meta">Add your API keys in Settings to preload source content here.</span>';
    return;
  }
  const gridClass = type === "feature" ? "featured-grid" : "trending-grid";
  target.innerHTML = sections
    .map((section) => {
      const cards = (section.items || []).map((item, index) => {
        const preview = item.preview || {};
        const thumb = pickThumb(preview);
        const thumbHtml = thumb.src
          ? `<div class="${type}-thumb"><img src="${escapeHtml(proxyMediaUrl(thumb.src))}" alt="" loading="lazy"></div>`
          : `<div class="${type}-thumb"></div>`;
        const buttonAttr = type === "feature" ? `data-feature-index="${index}" data-feature-source="${escapeHtml(section.source_id)}"` : `data-trend-tag="${escapeHtml(item.tag)}"`;
        const title = type === "feature" ? item.title : item.tag;
        const subtitle = type === "feature" ? item.subtitle : `Score ${formatNumber(item.count)}`;
        return `<button class="${type}-card" ${buttonAttr} type="button">${thumbHtml}<div class="${type}-copy"><h4>${escapeHtml(title)}</h4><p>${escapeHtml(subtitle)}</p></div></button>`;
      }).join("");
      return `<div class="source-section"><div class="source-head"><h3>${escapeHtml(section.source_name)}</h3><span class="source-label">${escapeHtml(section.source_id)}</span></div><div class="${gridClass}">${cards}</div></div>`;
    })
    .join("");
}

function renderNews(items) {
  if (!items?.length) {
    dom.newsList.innerHTML = '<span class="module-meta">No source news right now.</span>';
    return;
  }
  dom.newsList.innerHTML = items
    .map(
      (item) => `
        <article class="news-card">
          <span class="eyebrow">${escapeHtml(item.source_name || "App")}</span>
          <h4>${escapeHtml(item.title || "Untitled")}</h4>
          <p>${escapeHtml(item.summary || "")}</p>
          <a ${item.url ? `href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer"` : "href='#' data-stop-open='1'"}>${escapeHtml(item.published_at || "")}</a>
        </article>
      `
    )
    .join("");
}

function renderHome(payload) {
  state.home = payload;
  dom.homeStatus.textContent = payload?.updated_at ? `Updated ${payload.updated_at.replace("T", " ").slice(0, 16)}` : "Ready";
  renderRecentQueries(payload?.recent_queries || []);
  renderTrendingTabs(payload?.trending?.window || state.trendingWindow);
  renderHomeSections(dom.trendingSections, payload?.trending?.sections || [], "trend");
  renderHomeSections(dom.featuredSections, payload?.featured?.sections || [], "feature");
  renderNews(payload?.news?.items || []);
}

async function refreshHome(force = false) {
  if (!dom.adultCheckbox.checked) {
    dom.homeStatus.textContent = "Enable 18+ to load source content";
    renderRecentQueries([]);
    renderTrendingTabs(state.trendingWindow);
    dom.trendingSections.innerHTML = '<span class="module-meta">Age gate is required for home previews.</span>';
    dom.featuredSections.innerHTML = '<span class="module-meta">Add API keys and confirm 18+ to load featured posts.</span>';
    renderNews([]);
    return;
  }
  dom.homeStatus.textContent = force ? "Refreshing home..." : "Loading home...";
  const params = new URLSearchParams({ adult: "1", sources: selectedSources().join(",") });
  if (force) params.set("refresh", "1");
  try {
    const res = await fetch(`/api/home?${params.toString()}`, { headers: buildAuthHeaders() });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Home load failed");
    state.trendingWindow = data?.trending?.window || state.trendingWindow;
    renderHome(data);
  } catch (error) {
    dom.homeStatus.textContent = error.message || String(error);
  }
}

async function loadTrendingWindow(windowKey) {
  if (!dom.adultCheckbox.checked) return;
  const params = new URLSearchParams({ adult: "1", window: windowKey, sources: selectedSources().join(",") });
  try {
    const res = await fetch(`/api/trending?${params.toString()}`, { headers: buildAuthHeaders() });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || "Trending load failed");
    state.trendingWindow = data.window || windowKey;
    if (!state.home) state.home = {};
    state.home.trending = data;
    renderTrendingTabs(state.trendingWindow);
    renderHomeSections(dom.trendingSections, data.sections || [], "trend");
  } catch (error) {
    dom.homeStatus.textContent = error.message || String(error);
  }
}
function buildSearchParams(page) {
  const params = new URLSearchParams({
    adult: dom.adultCheckbox.checked ? "1" : "0",
    page: String(page),
    limit: dom.limitSelect.value,
    sort: dom.sortSelect.value,
    rating: dom.ratingSelect.value,
    min_score: dom.minScoreSelect.value,
    sources: selectedSources().join(","),
  });
  if (state.searchMode === "advanced") {
    const raw = normalizePhrase(dom.rawQueryInput.value);
    if (raw) params.set("raw_query", raw);
  } else {
    state.includeTags.forEach((tag) => params.append("include_tags", normalizePhrase(tag)));
    state.excludeTags.forEach((tag) => params.append("exclude_tags", normalizePhrase(tag)));
  }
  return params;
}

function currentResults() {
  return state.pageMap.get(state.currentPage) || [];
}

function renderResolvedQuery() {
  const meta = state.searchMeta;
  if (!meta) {
    dom.resolvedQueryBar.textContent = "Ready to search";
    dom.activeQuery.innerHTML = "";
    return;
  }
  const lines = Object.entries(meta.resolved_query || {})
    .map(([sourceId, query]) => `<span class="query-chip"><strong>${escapeHtml(sourceId)}</strong><small>${escapeHtml(query || "")}</small></span>`)
    .join("");
  dom.resolvedQueryBar.innerHTML = lines || "Ready to search";
  dom.activeQuery.innerHTML = lines || "";
}

function renderCard(item, index, scope) {
  const thumb = pickThumb(item);
  const thumbHtml = thumb.src
    ? thumb.kind === "video" && !runtimeAndroid
      ? `<div class="media-thumb"><video muted loop playsinline preload="none" data-hover-video="${escapeHtml(proxyMediaUrl(thumb.src))}"></video></div>`
      : `<div class="media-thumb"><img src="${escapeHtml(proxyMediaUrl(thumb.src))}" alt="" loading="lazy"></div>`
    : `<div class="media-thumb"></div>`;
  const likeClass = isFavorited(item) ? "liked" : "";
  return `
    <article class="media-card" data-open-card="${scope}:${index}">
      ${thumbHtml}
      <div class="media-copy">
        <div class="media-meta">
          <span class="source-pill">${escapeHtml(item.source_name || item.source_id)}</span>
          <span class="kind-pill">${escapeHtml(item.media_type || "image")}</span>
          <span class="score-pill">Score ${formatNumber(item.score)}</span>
        </div>
        <p>${escapeHtml((item.tags || "").slice(0, 160))}</p>
        <div class="media-actions">
          <button data-stop-open="1" data-download="${scope}:${index}" type="button">Download</button>
          <button class="${likeClass}" data-stop-open="1" data-like="${scope}:${index}" type="button">${isFavorited(item) ? "Liked" : "Like"}</button>
          <a class="media-link" data-stop-open="1" href="${escapeHtml(item.post_url || "#")}" target="_blank" rel="noreferrer">Open post</a>
        </div>
      </div>
    </article>
  `;
}

function renderResults() {
  const items = currentResults();
  const totalLoaded = [...state.pageMap.values()].reduce((sum, list) => sum + list.length, 0);
  dom.resultCount.textContent = `${items.length} on page ${state.currentPage + 1} / loaded ${totalLoaded}`;
  dom.grid.innerHTML = items.map((item, index) => renderCard(item, index, "results")).join("");
  renderResolvedQuery();
  renderPagination();
  bindHoverPreviews();
}

function renderPagination() {
  const pages = [...state.pageMap.keys()].sort((a, b) => a - b);
  if (!pages.length) {
    dom.paginationBar.innerHTML = "";
    return;
  }
  const buttons = pages.map((page) => `<button class="page-btn ${page === state.currentPage ? "active" : ""}" data-page="${page}" type="button">${page + 1}</button>`).join("");
  const nextPage = Math.max(...pages) + 1;
  const hasMore = state.pageHasMore.get(Math.max(...pages)) === true;
  dom.paginationBar.innerHTML = `${buttons}${hasMore ? `<button class="page-btn" data-next-page="1" type="button">+ ${nextPage + 1}</button>` : ""}`;
}

function renderFavorites() {
  const items = favoriteList();
  dom.favoritesTitle.textContent = `Liked media (${items.length})`;
  dom.favoritesGrid.innerHTML = items.length
    ? items.map((item, index) => renderCard(item, index, "favorites")).join("")
    : '<span class="module-meta">No favorites yet. Like posts from results or viewer.</span>';
  bindHoverPreviews();
}

async function loadPage(page, switchToPage = true) {
  if (!dom.adultCheckbox.checked) {
    setError("Confirm 18+ first.");
    return;
  }
  if (state.loading) return;
  if (state.pageMap.has(page)) {
    state.currentPage = page;
    renderResults();
    return;
  }
  const sessionAtStart = ++state.sessionId;
  state.loading = true;
  setStatus(`Loading page ${page + 1}...`);
  setError("");
  try {
    const params = buildSearchParams(page);
    const res = await fetch(`/api/search?${params.toString()}`, { headers: buildAuthHeaders() });
    const data = await res.json();
    if (sessionAtStart !== state.sessionId) return;
    if (!res.ok) throw new Error(data.error || "Search failed");
    state.searchMeta = data;
    state.pageMap.set(page, data.items || []);
    state.pageHasMore.set(page, Boolean(data.has_more));
    if (switchToPage) state.currentPage = page;
    setView("search");
    renderResults();
    setStatus(`Ready: page ${state.currentPage + 1}`);
    if (!data.items?.length && !(data.errors || []).length) {
      setError("No posts found for this page / filter set.");
    }
    if ((data.errors || []).length) {
      setError(data.errors.map((item) => `${item.source}: ${item.message}`).join(" | "));
    }
    refreshHome();
  } catch (error) {
    setError(error.message || String(error));
    setStatus("Search error");
  } finally {
    if (sessionAtStart === state.sessionId) state.loading = false;
  }
}

async function startSearch() {
  commitPendingBuilderInput();
  if (
    (state.searchMode === "builder" && !state.includeTags.length && !state.excludeTags.length) ||
    (state.searchMode === "advanced" && !normalizePhrase(dom.rawQueryInput.value))
  ) {
    setError(state.searchMode === "builder" ? "Type a tag phrase or pick a suggestion first." : "Enter a raw query first.");
    setStatus("Search blocked");
    return;
  }
  await saveSecureConfig();
  resetSearchState();
  await loadPage(0, true);
}

async function loadNextPage() {
  const pages = [...state.pageMap.keys()].sort((a, b) => a - b);
  const nextPage = pages.length ? pages[pages.length - 1] + 1 : 0;
  if (pages.length && state.pageHasMore.get(pages[pages.length - 1]) !== true) return;
  await loadPage(nextPage, false);
  state.currentPage = nextPage;
  renderResults();
}

function openViewer(items, index) {
  state.viewerItems = items;
  state.viewerIndex = index;
  renderViewer();
  dom.viewer.showModal();
}

function stopViewerMedia() {
  dom.mediaHolder.querySelectorAll("video").forEach((video) => {
    try {
      video.pause();
      video.removeAttribute("src");
      video.load();
    } catch {
      // no-op
    }
  });
}

function updateViewerLikeButton() {
  const item = state.viewerItems[state.viewerIndex];
  dom.likeCurrentBtn.classList.toggle("toggle-active", isFavorited(item));
  dom.likeCurrentBtn.textContent = isFavorited(item) ? "Liked" : "Like";
}

function renderViewer() {
  const item = state.viewerItems[state.viewerIndex];
  if (!item) return;
  stopViewerMedia();
  if (item.media_type === "video") {
    dom.mediaHolder.innerHTML = `<video src="${escapeHtml(proxyMediaUrl(item.file_url))}" controls autoplay loop playsinline></video>`;
  } else {
    dom.mediaHolder.innerHTML = `<img src="${escapeHtml(proxyMediaUrl(item.file_url))}" alt="media">`;
  }
  dom.metaBox.innerHTML = `
    <div><strong>${escapeHtml(item.source_name || item.source_id)}</strong> | ID ${escapeHtml(item.id)} | Score ${formatNumber(item.score)}</div>
    <div>${escapeHtml(item.width || "?")} x ${escapeHtml(item.height || "?")} | Rating ${escapeHtml(item.rating || "-")}</div>
    <div>${escapeHtml(item.tags || "")}</div>
    <div><a class="media-link" href="${escapeHtml(item.post_url || "#")}" target="_blank" rel="noreferrer">Open source post</a></div>
  `;
  updateViewerLikeButton();
}

function closeViewer() {
  stopViewerMedia();
  dom.viewer.close();
  state.viewerItems = [];
  state.viewerIndex = -1;
}

function moveViewer(step) {
  if (!state.viewerItems.length) return;
  state.viewerIndex = (state.viewerIndex + step + state.viewerItems.length) % state.viewerItems.length;
  renderViewer();
}
function resolveScopeItems(scope) {
  return scope === "favorites" ? favoriteList() : currentResults();
}

function handleContentClick(event) {
  const autoButton = event.target.closest("[data-auto-index]");
  if (autoButton) {
    applyAutocompleteItem(Number(autoButton.dataset.autoIndex));
    return;
  }
  const removeButton = event.target.closest("[data-remove-tag]");
  if (removeButton) {
    const [mode, index] = String(removeButton.dataset.removeTag || "include:0").split(":");
    removeTag(mode, Number(index));
    return;
  }
  const recentButton = event.target.closest("[data-recent-index]");
  if (recentButton) {
    const entry = state.home?.recent_queries?.[Number(recentButton.dataset.recentIndex)];
    if (entry) {
      populateQueryFromHistory(entry);
      setView("search");
    }
    return;
  }
  const featureButton = event.target.closest("[data-feature-index]");
  if (featureButton) {
    const section = state.home?.featured?.sections?.find((item) => item.source_id === featureButton.dataset.featureSource);
    const entry = section?.items?.[Number(featureButton.dataset.featureIndex)];
    if (entry) {
      setSearchMode("builder");
      state.includeTags = dedupe([entry.query]);
      state.excludeTags = [];
      renderSelectedTags();
      setView("search");
      startSearch();
    }
    return;
  }
  const trendButton = event.target.closest("[data-trend-tag]");
  if (trendButton) {
    addBuilderTag(String(trendButton.dataset.trendTag || ""), "include");
    setView("search");
    return;
  }
  const windowButton = event.target.closest("[data-window]");
  if (windowButton) {
    loadTrendingWindow(windowButton.dataset.window || "7d");
    return;
  }
  const pageButton = event.target.closest("[data-page]");
  if (pageButton) {
    state.currentPage = Number(pageButton.dataset.page || 0);
    renderResults();
    return;
  }
  if (event.target.closest("[data-next-page]")) {
    loadNextPage();
    return;
  }
  const downloadButton = event.target.closest("[data-download]");
  if (downloadButton) {
    const [scope, index] = String(downloadButton.dataset.download || "results:0").split(":");
    const item = resolveScopeItems(scope)[Number(index)];
    downloadItem(item);
    return;
  }
  const likeButton = event.target.closest("[data-like]");
  if (likeButton) {
    const [scope, index] = String(likeButton.dataset.like || "results:0").split(":");
    const item = resolveScopeItems(scope)[Number(index)];
    toggleFavorite(item);
    return;
  }
  const openCard = event.target.closest("[data-open-card]");
  if (openCard && !event.target.closest("[data-stop-open]")) {
    const [scope, index] = String(openCard.dataset.openCard || "results:0").split(":");
    openViewer(resolveScopeItems(scope), Number(index));
  }
}

function bindHoverPreviews() {
  if (runtimeAndroid) return;
  dom.grid.querySelectorAll("[data-hover-video]").forEach((video) => {
    const src = video.dataset.hoverVideo;
    if (!src) return;
    const start = () => {
      if (!video.src) video.src = src;
      video.play().catch(() => {});
    };
    const stop = () => {
      video.pause();
      video.currentTime = 0;
    };
    video.closest(".media-card")?.addEventListener("mouseenter", start);
    video.closest(".media-card")?.addEventListener("mouseleave", stop);
  });
}

function wireEvents() {
  document.addEventListener("click", handleContentClick);
  dom.topNav.addEventListener("click", (event) => {
    const button = event.target.closest("[data-view]");
    if (!button) return;
    setView(button.dataset.view === "search" ? "search" : button.dataset.view);
  });
  dom.mobileNav.addEventListener("click", (event) => {
    const button = event.target.closest("[data-view]");
    if (!button) return;
    setView(button.dataset.view === "search" ? "search" : button.dataset.view);
  });
  dom.builderTabBtn.addEventListener("click", () => setSearchMode("builder"));
  dom.advancedTabBtn.addEventListener("click", () => setSearchMode("advanced"));
  dom.builderModeBtn.addEventListener("click", () => setBuilderMode(state.builderMode === "include" ? "exclude" : "include"));
  dom.addTagBtn.addEventListener("click", () => addBuilderTag(dom.builderInput.value));
  dom.builderInput.addEventListener("input", queueAutocomplete);
  dom.builderInput.addEventListener("focus", queueAutocomplete);
  dom.builderInput.addEventListener("keydown", (event) => {
    if (event.key === "Escape") hideAutocomplete();
    if (event.key === "Enter") {
      event.preventDefault();
      if (!dom.autocompleteMenu.classList.contains("hidden") && state.autocompleteItems.length) {
        applyAutocompleteItem(0);
      } else {
        addBuilderTag(dom.builderInput.value);
      }
    }
  });
  dom.searchBtn.addEventListener("click", startSearch);
  dom.clearQueryBtn.addEventListener("click", clearQuery);
  dom.loadMoreBtn.addEventListener("click", loadNextPage);
  dom.favoritesBtn.addEventListener("click", () => setView(state.view === "favorites" ? "search" : "favorites"));
  dom.openDownloadsBtn.addEventListener("click", openDownloadsFolder);
  dom.openSettingsBtn.addEventListener("click", () => setView("settings"));
  dom.refreshHomeBtn.addEventListener("click", () => refreshHome(true));
  dom.clearSecureBtn.addEventListener("click", async () => {
    await fetch("/api/secure-config/clear", { method: "POST" });
    applySecurePayload({});
    refreshSecurityStatus();
  });
  dom.themeSelect.addEventListener("change", () => applyTheme(dom.themeSelect.value));
  [dom.sourceSelect, dom.sortSelect, dom.ratingSelect, dom.minScoreSelect, dom.limitSelect, dom.adultCheckbox].forEach((el) => {
    el.addEventListener("change", () => {
      savePrefs();
      refreshHome();
    });
  });
  [dom.rule34UserId, dom.rule34ApiKey, dom.gelbooruUserId, dom.gelbooruApiKey, dom.proxyUrl].forEach((el) => {
    el.addEventListener("input", queueSecureSave);
  });
  dom.downloadCurrentBtn.addEventListener("click", () => downloadItem(state.viewerItems[state.viewerIndex]));
  dom.likeCurrentBtn.addEventListener("click", () => toggleFavorite(state.viewerItems[state.viewerIndex]));
  dom.prevBtn.addEventListener("click", () => moveViewer(-1));
  dom.nextBtn.addEventListener("click", () => moveViewer(1));
  dom.closeBtn.addEventListener("click", closeViewer);
  dom.viewer.addEventListener("close", stopViewerMedia);
}

async function boot() {
  loadFavorites();
  updateFavoritesButton();
  renderSelectedTags();
  wireEvents();
  await fetchSources();
  loadPrefs();
  await Promise.all([loadSecureConfig(), refreshSecurityStatus()]);
  renderFavorites();
  renderResolvedQuery();
  setStatus("Ready");
  await refreshHome();
  if (runtimeAndroid) {
    setView("home");
  }
}

boot();
