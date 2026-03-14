const state = {
  currentPage: 0,
  pageMap: new Map(),
  pageHasMore: new Map(),
  filteredItems: [],
  performanceMode: false,
  viewMode: "search",
  favorites: new Map(),
  viewerIndex: -1,
  loading: false,
  sessionId: 0,
  hintTimer: null,
  activeToken: null,
  hintAbortController: null,
  hintCache: new Map(),
  hintRequestId: 0,
  secureSaveTimer: null,
  downloadBusy: false,
};

const dom = {
  tagsInput: document.getElementById("tagsInput"),
  tagHints: document.getElementById("tagHints"),
  blacklistInput: document.getElementById("blacklistInput"),
  sortSelect: document.getElementById("sortSelect"),
  limitSelect: document.getElementById("limitSelect"),
  minScoreSelect: document.getElementById("minScoreSelect"),
  adultCheckbox: document.getElementById("adultCheckbox"),
  rule34UserId: document.getElementById("rule34UserId"),
  rule34ApiKey: document.getElementById("rule34ApiKey"),
  gelbooruUserId: document.getElementById("gelbooruUserId"),
  gelbooruApiKey: document.getElementById("gelbooruApiKey"),
  proxyUrl: document.getElementById("proxyUrl"),
  securityStatus: document.getElementById("securityStatus"),
  clearSecureBtn: document.getElementById("clearSecureBtn"),
  sourceSelect: document.getElementById("sourceSelect"),
  themeSelect: document.getElementById("themeSelect"),
  favoritesBtn: document.getElementById("favoritesBtn"),
  searchBtn: document.getElementById("searchBtn"),
  loadMoreBtn: document.getElementById("loadMoreBtn"),
  openDownloadsBtn: document.getElementById("openDownloadsBtn"),
  resultCount: document.getElementById("resultCount"),
  statusText: document.getElementById("statusText"),
  grid: document.getElementById("grid"),
  paginationBar: document.getElementById("paginationBar"),
  scrollSentinel: document.getElementById("scrollSentinel"),
  errorBox: document.getElementById("errorBox"),
  viewer: document.getElementById("viewer"),
  mediaHolder: document.getElementById("mediaHolder"),
  metaBox: document.getElementById("metaBox"),
  prevBtn: document.getElementById("prevBtn"),
  nextBtn: document.getElementById("nextBtn"),
  downloadCurrentBtn: document.getElementById("downloadCurrentBtn"),
  likeCurrentBtn: document.getElementById("likeCurrentBtn"),
  closeBtn: document.getElementById("closeBtn"),
};

const PREF_KEYS = ["sortSelect", "limitSelect", "sourceSelect", "themeSelect", "tagsInput", "blacklistInput", "adultCheckbox"];
const AUTH_REQUIRED = {
  rule34: true,
  gelbooru: true,
};
const FAVORITES_KEY = "bf_favorites_v1";
const runtimeQuery = new URLSearchParams(window.location.search);
const runtimeAndroid = runtimeQuery.get("android") === "1" || /Android/i.test(navigator.userAgent || "");
state.performanceMode = runtimeAndroid;

const THEME_PROFILES = {
  old_neko: {
    labels: {
      search: "Поиск",
      loadMore: "Загрузить еще",
      downloads: "Папка загрузок",
      openPost: "Пост",
      download: "Скачать",
      favorites: "Любимое",
      like: "Лайк видео",
      liked: "В любимом",
    },
    chrome: "neko",
  },
  retro_blue: {
    labels: {
      search: "RUN QUERY",
      loadMore: "NEXT BLOCK",
      downloads: "OPEN CACHE DIR",
      openPost: "SOURCE",
      download: "SAVE BIN",
      favorites: "FAVORITES",
      like: "LIKE VID",
      liked: "LIKED",
    },
    chrome: "retro",
  },
};

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function isVideoUrl(url) {
  const value = String(url || "").toLowerCase();
  return [".mp4", ".webm", ".mov", ".m4v", ".avi", ".mkv"].some((ext) => value.includes(ext));
}

function pickFirstImageUrl(...urls) {
  for (const value of urls) {
    const url = String(value || "").trim();
    if (!url) continue;
    if (isVideoUrl(url)) continue;
    return url;
  }
  return "";
}

function normalizeTagQuery(value) {
  const compact = String(value || "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!compact) return "";
  if (!compact.includes(",")) return compact;

  return compact
    .split(",")
    .map((chunk) => chunk.trim())
    .filter(Boolean)
    .map((tag) => {
      const negative = tag.startsWith("-");
      const raw = negative ? tag.slice(1).trim() : tag;
      const normalized = raw
        .split(/\s+/)
        .filter(Boolean)
        .join("_");
      return negative ? `-${normalized}` : normalized;
    })
    .filter(Boolean)
    .join(" ");
}

function normalizeHintInput(value) {
  return String(value || "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeTagInputDisplay(value) {
  const compact = normalizeHintInput(value);
  if (!compact) return "";
  if (!compact.includes(",")) return compact;
  return compact
    .split(",")
    .map((chunk) => normalizeHintInput(chunk))
    .filter(Boolean)
    .join(", ");
}

function formatHintCount(value) {
  return new Intl.NumberFormat("ru-RU").format(Number(value || 0));
}

function usesRawTokenHintMode(value) {
  const compact = normalizeHintInput(value);
  if (!compact || compact.includes(",")) return false;
  return compact.split(/\s+/).some((token) => /[_:~\d]/.test(token));
}

function pickCardThumb(item) {
  const preview = item.preview_url || "";
  const sample = item.sample_url || "";
  const file = item.file_url || "";
  const fullImage = pickFirstImageUrl(file, sample, preview);
  const poster = pickFirstImageUrl(sample, preview, file);

  if (item.media_type === "video") {
    if (state.performanceMode) {
      if (poster) return { kind: "image", src: poster };
      return { kind: "placeholder" };
    }
    // Prefer image poster when available; otherwise render a muted video clip.
    if (sample && !isVideoUrl(sample)) return { kind: "image", src: sample };
    if (preview && !isVideoUrl(preview)) return { kind: "image", src: preview };
    if (sample || file) return { kind: "video", src: sample || file, poster: poster || "" };
    return { kind: "placeholder" };
  }

  return { kind: "image", src: fullImage || sample || preview || file };
}

function proxyMediaUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  return `/api/media?url=${encodeURIComponent(raw)}`;
}

function setStatus(text) {
  dom.statusText.textContent = text;
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

function applyTheme(themeName) {
  const nextTheme = Object.prototype.hasOwnProperty.call(THEME_PROFILES, themeName)
    ? themeName
    : "old_neko";
  const profile = THEME_PROFILES[nextTheme];
  document.body.setAttribute("data-theme", nextTheme);
  document.body.setAttribute("data-theme-chrome", profile.chrome);
  if (dom.themeSelect.value !== nextTheme) {
    dom.themeSelect.value = nextTheme;
  }
  dom.searchBtn.textContent = profile.labels.search;
  dom.loadMoreBtn.textContent = profile.labels.loadMore;
  dom.openDownloadsBtn.textContent = profile.labels.downloads;
  dom.downloadCurrentBtn.textContent = profile.labels.download;
  if (state.viewerIndex >= 0) {
    updateViewerLikeButton();
  } else {
    dom.likeCurrentBtn.textContent = profile.labels.like;
  }
  updateFavoritesButton();
}

function formatBytes(value) {
  const size = Number(value || 0);
  if (!Number.isFinite(size) || size <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let n = size;
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i += 1;
  }
  return `${n.toFixed(n >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
}

async function downloadItem(item) {
  if (!item?.file_url) {
    setError("Нет URL файла для скачивания");
    return;
  }
  if (state.downloadBusy) return;
  state.downloadBusy = true;
  setStatus(`Скачивание: ${item.id}...`);
  setError("");

  try {
    const res = await fetch("/api/download", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...buildAuthHeaders(),
      },
      body: JSON.stringify({
        url: item.file_url,
        source_id: item.source_id,
        post_id: item.id,
      }),
    });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error(data.error || "Не удалось скачать файл");
    }
    setStatus(`Скачано: ${data.filename} (${formatBytes(data.size)})`);
    setError("");
  } catch (error) {
    setStatus("Ошибка скачивания");
    setError(error.message || String(error));
  } finally {
    state.downloadBusy = false;
  }
}

function selectedSources() {
  const value = (dom.sourceSelect.value || "rule34").trim();
  if (value === "both") return ["rule34", "gelbooru"];
  return [value];
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

function missingAuthSources(sources) {
  const missing = [];
  for (const source of sources) {
    if (!AUTH_REQUIRED[source]) continue;
    if (source === "rule34" && (!dom.rule34UserId.value.trim() || !dom.rule34ApiKey.value.trim())) {
      missing.push("Rule34");
    }
    if (source === "gelbooru" && (!dom.gelbooruUserId.value.trim() || !dom.gelbooruApiKey.value.trim())) {
      missing.push("Gelbooru");
    }
  }
  return missing;
}

function savePreferences() {
  PREF_KEYS.forEach((key) => {
    const el = dom[key];
    if (!el) return;
    const value = el.type === "checkbox" ? String(el.checked) : el.value;
    localStorage.setItem(`bf_${key}`, value);
  });
}

function loadPreferences() {
  PREF_KEYS.forEach((key) => {
    const el = dom[key];
    const saved = localStorage.getItem(`bf_${key}`);
    if (!el || saved === null) return;
    if (el.type === "checkbox") {
      el.checked = saved === "true";
    } else {
      el.value = saved;
    }
  });
}

function collectSecurePayload() {
  return {
    credentials: {
      rule34: {
        user_id: dom.rule34UserId.value.trim(),
        api_key: dom.rule34ApiKey.value.trim(),
      },
      gelbooru: {
        user_id: dom.gelbooruUserId.value.trim(),
        api_key: dom.gelbooruApiKey.value.trim(),
      },
    },
    network: {
      proxy_url: dom.proxyUrl.value.trim(),
    },
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
    if (!res.ok) {
      dom.securityStatus.textContent = "Security: unavailable";
      return;
    }
    const storage = String(data.storage || "").toUpperCase();
    const proxy = data.proxy_active ? "proxy on" : "proxy off";
    dom.securityStatus.textContent = `Security: ${storage} storage, HTTPS only, ${proxy}`;
  } catch {
    dom.securityStatus.textContent = "Security: unavailable";
  }
}

async function loadSecureConfig() {
  try {
    const res = await fetch("/api/secure-config");
    const data = await res.json();
    if (!res.ok) return;
    applySecurePayload(data);
  } catch {
    // no-op, keep empty fields
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
  state.secureSaveTimer = setTimeout(saveSecureConfig, 300);
}

function blacklistTags() {
  return dom.blacklistInput.value
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);
}

function isBlocked(item, blacklist) {
  if (!blacklist.length) return false;
  const tagSet = new Set(item.tags.toLowerCase().split(/\s+/));
  return blacklist.some((blocked) => tagSet.has(blocked));
}

function loadedPages() {
  return [...state.pageMap.keys()].sort((a, b) => a - b);
}

function maxLoadedPage() {
  const pages = loadedPages();
  return pages.length ? pages[pages.length - 1] : -1;
}

function hasMoreFromEnd() {
  const maxPage = maxLoadedPage();
  if (maxPage < 0) return false;
  return state.pageHasMore.get(maxPage) === true;
}

function currentRawItems() {
  return state.pageMap.get(state.currentPage) || [];
}

function currentThemeProfile() {
  const key = document.body.getAttribute("data-theme") || "old_neko";
  return THEME_PROFILES[key] || THEME_PROFILES.old_neko;
}

function favoriteKey(item) {
  return `${item.source_id}:${item.id}`;
}

function isFavorited(item) {
  if (!item) return false;
  return state.favorites.has(favoriteKey(item));
}

function favoriteList() {
  return [...state.favorites.values()].sort((a, b) => Number(b.favorite_ts || 0) - Number(a.favorite_ts || 0));
}

function saveFavorites() {
  try {
    localStorage.setItem(FAVORITES_KEY, JSON.stringify(favoriteList()));
  } catch {
    // no-op
  }
}

function loadFavorites() {
  state.favorites.clear();
  try {
    const raw = localStorage.getItem(FAVORITES_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return;
    parsed.forEach((item) => {
      if (!item || item.media_type !== "video") return;
      state.favorites.set(favoriteKey(item), item);
    });
  } catch {
    // no-op
  }
}

function updateFavoritesButton() {
  const profile = currentThemeProfile();
  const total = state.favorites.size;
  dom.favoritesBtn.textContent = `${profile.labels.favorites} (${total})`;
  dom.favoritesBtn.classList.toggle("toggle-active", state.viewMode === "favorites");
}

function likeLabel(item) {
  const profile = currentThemeProfile();
  return isFavorited(item) ? profile.labels.liked : profile.labels.like;
}

function toggleFavorite(item) {
  if (!item || item.media_type !== "video") {
    setError("В Favorites добавляются только видео.");
    return;
  }

  const key = favoriteKey(item);
  if (state.favorites.has(key)) {
    state.favorites.delete(key);
    setStatus("Видео удалено из Favorites");
  } else {
    state.favorites.set(key, {
      ...item,
      favorite_ts: Date.now(),
    });
    setStatus("Видео добавлено в Favorites");
  }

  saveFavorites();
  updateFavoritesButton();
  renderGrid();
  renderPagination();
  updateViewerLikeButton();
}

function showFavorites() {
  state.viewMode = "favorites";
  renderGrid();
  renderPagination();
  updateFavoritesButton();
  setStatus("Режим Favorites");
  if (!state.filteredItems.length) {
    setError("В Favorites пока нет видео.");
  } else {
    setError("");
  }
}

function showSearchResults() {
  state.viewMode = "search";
  renderGrid();
  renderPagination();
  updateFavoritesButton();
  setStatus("Режим поиска");
  setError("");
}

function renderGrid() {
  const blacklist = blacklistTags();
  if (state.viewMode === "favorites") {
    state.filteredItems = favoriteList();
  } else {
    state.filteredItems = currentRawItems().filter((item) => !isBlocked(item, blacklist));
  }
  const profile = currentThemeProfile();

  if (state.viewMode === "favorites") {
    dom.resultCount.textContent = `${state.filteredItems.length} favorites videos`;
  } else {
    const totalLoadedItems = [...state.pageMap.values()].reduce((sum, items) => sum + items.length, 0);
  dom.resultCount.textContent = `${state.filteredItems.length} на стр. ${state.currentPage + 1} | загружено ${totalLoadedItems}`;

  }

  if (!state.filteredItems.length) {
    dom.grid.innerHTML = "";
    return;
  }

  dom.grid.innerHTML = state.filteredItems
    .map((item, index) => {
      const thumb = pickCardThumb(item);
      const tagSnippet = escapeHtml(item.tags.slice(0, 220));
      const mediaBadge = item.media_type === "video" ? "video" : "image";
      const rating = escapeHtml(item.rating || "-");
      const size = `${item.width || "?"}x${item.height || "?"}`;
      const postUrl = escapeHtml(item.post_url || "#");
      const likeButton =
        item.media_type === "video"
          ? `<button class="action-btn like-btn ${isFavorited(item) ? "liked" : ""}" data-like-index="${index}" type="button">${escapeHtml(likeLabel(item))}</button>`
          : "";
      const mediaPreview =
        thumb.kind === "video"
          ? `<video class="thumb" src="${escapeHtml(proxyMediaUrl(thumb.src))}" data-direct-src="${escapeHtml(thumb.src)}" poster="${escapeHtml(proxyMediaUrl(thumb.poster || "") || "")}" muted loop autoplay playsinline preload="metadata"></video>`
          : thumb.kind === "image"
            ? `<img class="thumb" src="${escapeHtml(proxyMediaUrl(thumb.src))}" data-direct-src="${escapeHtml(thumb.src)}" alt="" loading="lazy" decoding="async" fetchpriority="low">`
            : `<div class="thumb thumb-placeholder">video</div>`;
      return `
        <article class="card" data-index="${index}" style="--stagger:${index}; --lamp-seed:${index % 7}">
          <div class="tv-shell">
            <div class="tv-screen">
              ${mediaPreview}
              <span class="tv-glare"></span>
              <span class="tv-static"></span>
            </div>
            <div class="tv-panel">
              <span class="tv-led led-red"></span>
              <span class="tv-led led-amber"></span>
              <span class="tv-led led-green"></span>
            </div>
          </div>
          <div class="meta">
            <div class="line">
              <span class="chip">${escapeHtml(item.source_name)}</span>
              <span class="chip">${mediaBadge}</span>
            </div>
            <div class="line">
              <strong>Score: ${item.score}</strong>
              <span>ID: ${escapeHtml(item.id)}</span>
            </div>
            <div class="line">
              <span>Rating: ${rating}</span>
              <span>${size}</span>
            </div>
            <div class="actions">
              <button class="action-btn" data-download-index="${index}" type="button">${escapeHtml(profile.labels.download)}</button>
              <a class="action-btn" href="${postUrl}" target="_blank" rel="noreferrer" data-stop-open="1">${escapeHtml(profile.labels.openPost)}</a>
              ${likeButton}
            </div>
            <div class="tagline">${tagSnippet}</div>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderPagination() {
  if (state.viewMode === "favorites") {
    dom.paginationBar.innerHTML = "";
    return;
  }

  const pages = loadedPages();
  if (!pages.length) {
    dom.paginationBar.innerHTML = "";
    return;
  }

  const buttons = pages
    .map((page) => {
      const activeClass = page === state.currentPage ? "active" : "";
      return `<button class="page-btn ${activeClass}" data-page="${page}">${page + 1}</button>`;
    })
    .join("");

  const nextPage = maxLoadedPage() + 1;
  const moreButton = hasMoreFromEnd()
    ? `<button class="page-btn" data-load-next="1">+ стр. ${nextPage + 1}</button>`
    : `<button class="page-btn" disabled>Конец</button>`;

  dom.paginationBar.innerHTML = `${buttons}${moreButton}`;
}

function updateViewerLikeButton() {
  const item = state.filteredItems[state.viewerIndex];
  if (!item || item.media_type !== "video") {
    dom.likeCurrentBtn.classList.add("hidden");
    return;
  }
  dom.likeCurrentBtn.classList.remove("hidden");
  dom.likeCurrentBtn.textContent = likeLabel(item);
  dom.likeCurrentBtn.classList.toggle("toggle-active", isFavorited(item));
}

function renderViewerItem(index) {
  const item = state.filteredItems[index];
  if (!item) return;

  state.viewerIndex = index;
  stopViewerMedia();
  dom.mediaHolder.innerHTML = "";

  if (item.media_type === "video") {
    const video = document.createElement("video");
    video.src = proxyMediaUrl(item.file_url);
    video.dataset.directSrc = item.file_url;
    video.controls = true;
    video.autoplay = true;
    video.loop = true;
    video.playsInline = true;
    video.setAttribute("playsinline", "");
    video.muted = true;
    video.preload = "metadata";
    video.addEventListener("loadedmetadata", () => {
      video.play().catch(() => {
        // ignore autoplay block, user can start with controls
      });
    });
    video.addEventListener("error", () => {
      if (video.dataset.directSrc && video.src !== video.dataset.directSrc) {
        video.src = video.dataset.directSrc;
      }
    });
    dom.mediaHolder.appendChild(video);
  } else {
    const img = document.createElement("img");
    img.src = proxyMediaUrl(item.file_url);
    img.dataset.directSrc = item.file_url;
    img.alt = "media";
    img.addEventListener("error", () => {
      if (img.dataset.directSrc && img.src !== img.dataset.directSrc) {
        img.src = img.dataset.directSrc;
      }
    });
    dom.mediaHolder.appendChild(img);
  }

  dom.metaBox.innerHTML = `
    <div>Источник: <strong>${escapeHtml(item.source_name)}</strong> | Score: <strong>${item.score}</strong> | Rating: <strong>${escapeHtml(item.rating || "-")}</strong></div>
    <div>ID: ${escapeHtml(item.id)} | Размер: ${item.width || "?"}x${item.height || "?"}</div>
    <div>Теги: ${escapeHtml(item.tags)}</div>
    <div><a href="${escapeHtml(item.post_url)}" target="_blank" rel="noreferrer">Открыть пост на сайте</a></div>
  `;
  updateViewerLikeButton();
}

function openViewer(index) {
  renderViewerItem(index);
  dom.viewer.showModal();
}

function stopViewerMedia() {
  const videos = dom.mediaHolder.querySelectorAll("video");
  videos.forEach((video) => {
    try {
      video.pause();
      video.currentTime = 0;
      video.removeAttribute("src");
      video.load();
    } catch {
      // no-op
    }
  });
}

function closeViewer() {
  stopViewerMedia();
  dom.viewer.close();
  state.viewerIndex = -1;
  dom.likeCurrentBtn.classList.remove("toggle-active");
  dom.likeCurrentBtn.classList.add("hidden");
}

function nextViewer(step) {
  if (!state.filteredItems.length) return;
  const total = state.filteredItems.length;
  const next = (state.viewerIndex + step + total) % total;
  renderViewerItem(next);
}

function resetPages() {
  state.currentPage = 0;
  state.pageMap.clear();
  state.pageHasMore.clear();
  state.filteredItems = [];
  state.viewerIndex = -1;
}

function buildSearchParams(page) {
  const tags = normalizeTagQuery(dom.tagsInput.value);
  const params = new URLSearchParams({
    tags,
    page: String(page),
    limit: dom.limitSelect.value,
    min_score: dom.minScoreSelect.value,
    sort: dom.sortSelect.value,
    sources: selectedSources().join(","),
    adult: dom.adultCheckbox.checked ? "1" : "0",
  });
  return params;
}

async function loadPage(page, switchToPage = true) {
  if (!dom.adultCheckbox.checked) {
    setError("Нужно подтвердить 18+.");
    return;
  }
  if (state.loading) {
    return;
  }

  const sources = selectedSources();
  if (!sources.length) {
    setError("Выберите хотя бы один источник.");
    return;
  }
  const missing = missingAuthSources(sources);
  if (missing.length) {
    setError(`Заполни user_id + api_key для: ${missing.join(", ")}`);
    return;
  }

  if (state.pageMap.has(page)) {
    state.currentPage = page;
    renderGrid();
    renderPagination();
    return;
  }

  const sessionAtStart = state.sessionId;
  state.loading = true;
  setStatus(`Загрузка страницы ${page + 1}...`);
  setError("");

  try {
    const params = buildSearchParams(page);
    const res = await fetch(`/api/search?${params.toString()}`, {
      headers: buildAuthHeaders(),
    });
    const data = await res.json();

    if (sessionAtStart !== state.sessionId) {
      return;
    }

    if (!res.ok) {
      throw new Error(data.error || "Ошибка API");
    }

    state.pageMap.set(page, data.items || []);
    state.pageHasMore.set(page, Boolean(data.has_more));

    if (switchToPage) {
      state.currentPage = page;
    }

    renderGrid();
    renderPagination();

    const sourceErrors = data.errors?.map((e) => `${e.source}: ${e.message}`).join(" | ");
    const rawCount = Number(data.raw_count || 0);
    const filteredCount = Number(data.count || 0);
    const minScore = Number(dom.minScoreSelect.value || 0);

    if (!sourceErrors && filteredCount === 0 && rawCount > 0 && minScore > 0) {
      setError(`Найдено ${rawCount}, но скрыто фильтром score >= ${minScore}. Поставь "Любой score".`);
    } else if (!sourceErrors && filteredCount === 0 && rawCount === 0) {
      setError("По этому тегу на текущей странице ничего не найдено.");
    }

    if (sourceErrors) {
      setError(sourceErrors);
      setStatus(`Частичные ошибки: ${sourceErrors}`);
    } else {
      if (!(filteredCount === 0 && (rawCount > 0 || rawCount === 0))) {
        setError("");
      }
      setStatus(`Готово: страница ${page + 1}`);
    }
  } catch (error) {
    setStatus("Ошибка запроса");
    setError(error.message || String(error));
  } finally {
    if (sessionAtStart === state.sessionId) {
      state.loading = false;
    }
  }
}

async function startSearch() {
  const normalizedTags = normalizeTagInputDisplay(dom.tagsInput.value);
  if (dom.tagsInput.value !== normalizedTags) {
    dom.tagsInput.value = normalizedTags;
    savePreferences();
  }
  await saveSecureConfig();
  state.viewMode = "search";
  state.sessionId += 1;
  resetPages();
  renderGrid();
  renderPagination();
  updateFavoritesButton();
  await loadPage(0, true);
}

async function loadNextPage(moveToLoadedPage = true) {
  if (state.viewMode === "favorites") {
    return;
  }
  const nextPage = maxLoadedPage() + 1;
  if (nextPage < 0) {
    await loadPage(0, true);
    return;
  }
  if (!hasMoreFromEnd() && state.pageMap.has(maxLoadedPage())) {
    return;
  }
  await loadPage(nextPage, moveToLoadedPage);
}

function hideHints() {
  state.activeToken = null;
  state.hintRequestId += 1;
  state.hintAbortController?.abort();
  state.hintAbortController = null;
  dom.tagHints.innerHTML = "";
  dom.tagHints.classList.add("hidden");
}

function activeTokenInfo() {
  const value = dom.tagsInput.value;
  const caret = dom.tagsInput.selectionStart ?? value.length;
  const left = value.slice(0, caret);
  const separatorIndex = left.lastIndexOf(",");
  let chunkStart = separatorIndex >= 0 ? separatorIndex + 1 : 0;
  let chunk = left.slice(chunkStart);
  let mode = separatorIndex >= 0 ? "comma_list" : "phrase";

  if (separatorIndex < 0 && usesRawTokenHintMode(left)) {
    const tokenMatch = left.match(/(?:^|\s+)(-?[^\s]+)$/);
    if (!tokenMatch) return null;
    chunk = tokenMatch[1] || "";
    chunkStart = caret - chunk.length;
    mode = "raw_token";
  }

  const trimmedChunk = chunk.replace(/^\s+/, "");
  const start = chunkStart + (chunk.length - trimmedChunk.length);
  const isNegative = trimmedChunk.startsWith("-");
  const raw = normalizeHintInput(isNegative ? trimmedChunk.slice(1) : trimmedChunk);

  if (raw.length < 2) return null;
  return { start, end: caret, isNegative, raw, mode };
}

async function fetchTagHints() {
  const tokenInfo = activeTokenInfo();
  state.activeToken = tokenInfo;
  if (!tokenInfo) {
    hideHints();
    return;
  }
  if (!dom.adultCheckbox.checked) {
    hideHints();
    return;
  }

  const sources = selectedSources();
  if (!sources.length) {
    hideHints();
    return;
  }
  if (missingAuthSources(sources).length) {
    hideHints();
    return;
  }

  const params = new URLSearchParams({
    term: tokenInfo.raw,
    limit: "12",
    sources: sources.join(","),
    adult: "1",
  });
  const cacheKey = `${sources.join(",")}|${tokenInfo.isNegative ? "-" : ""}${tokenInfo.raw.toLowerCase()}`;
  const cached = state.hintCache.get(cacheKey);
  if (cached) {
    dom.tagHints.innerHTML = cached;
    dom.tagHints.classList.remove("hidden");
    return;
  }

  state.hintRequestId += 1;
  const requestId = state.hintRequestId;
  state.hintAbortController?.abort();
  const controller = new AbortController();
  state.hintAbortController = controller;

  try {
    const res = await fetch(`/api/tags?${params.toString()}`, {
      headers: buildAuthHeaders(),
      signal: controller.signal,
    });
    const data = await res.json();
    if (requestId !== state.hintRequestId) return;
    if (!res.ok) {
      hideHints();
      return;
    }
    const hints = data.suggestions || [];
    if (!hints.length) {
      hideHints();
      return;
    }

    dom.tagHints.innerHTML = hints
      .map(
        (hint) => `
        <button class="hint-item" data-tag="${escapeHtml(hint.name)}">
          <span class="hint-left">
            <span class="hint-tag">${escapeHtml(hint.name)}</span>
            <span class="hint-site">${escapeHtml(hint.source_name)}</span>
          </span>
          <span class="hint-count">${formatHintCount(hint.count)}</span>
        </button>
      `
      )
      .join("");
    state.hintCache.set(cacheKey, dom.tagHints.innerHTML);
    dom.tagHints.classList.remove("hidden");
  } catch (error) {
    if (error?.name === "AbortError") return;
    hideHints();
  }
}

function applyTagHint(tag) {
  const tokenInfo = state.activeToken;
  if (!tokenInfo) return;

  const value = dom.tagsInput.value;
  const before = value.slice(0, tokenInfo.start);
  const after = value.slice(tokenInfo.end);
  const finalTag = tokenInfo.isNegative ? `-${tag}` : tag;

  if (tokenInfo.mode === "raw_token") {
    const cleanedAfter = after.replace(/^\s+/, "");
    const next = cleanedAfter ? `${before}${finalTag} ${cleanedAfter}` : `${before}${finalTag}`;
    dom.tagsInput.value = next;
    const caret = `${before}${finalTag}`.length;
    dom.tagsInput.focus();
    dom.tagsInput.setSelectionRange(caret, caret);
    hideHints();
    savePreferences();
    return;
  }

  const cleanedBefore = before.replace(/\s*$/, before.trimEnd().endsWith(",") ? " " : "");
  const cleanedAfter = after.replace(/^[\s,]+/, "");
  const suffix = cleanedAfter ? `, ${cleanedAfter}` : ", ";
  const next = `${cleanedBefore}${finalTag}${suffix}`;
  dom.tagsInput.value = next;

  const caret = `${cleanedBefore}${finalTag}, `.length;
  dom.tagsInput.focus();
  dom.tagsInput.setSelectionRange(caret, caret);
  hideHints();
  savePreferences();
}

function queueTagHints() {
  clearTimeout(state.hintTimer);
  state.hintTimer = setTimeout(fetchTagHints, 90);
}

function setupAutoPaging() {
  if (state.performanceMode) {
    return;
  }
  const observer = new IntersectionObserver(
    (entries) => {
      const hit = entries.some((entry) => entry.isIntersecting);
      if (!hit) return;
      if (state.loading) return;
      const maxPage = maxLoadedPage();
      if (maxPage < 0) return;
      if (!hasMoreFromEnd()) return;
      const shouldSwitch = state.currentPage === maxPage;
      loadNextPage(shouldSwitch);
    },
    { rootMargin: "400px 0px 400px 0px" }
  );
  observer.observe(dom.scrollSentinel);
}

async function fetchSources() {
  const res = await fetch("/api/sources");
  const data = await res.json();
  const previousValue = dom.sourceSelect.value || localStorage.getItem("bf_sourceSelect") || "rule34";
  const options = (data.sources || []).map((s) => `<option value="${s.id}">${s.name}</option>`);
  options.push('<option value="both">Rule34 + Gelbooru</option>');
  dom.sourceSelect.innerHTML = options.join("");
  if ([...dom.sourceSelect.options].some((o) => o.value === previousValue)) {
    dom.sourceSelect.value = previousValue;
  } else if ([...dom.sourceSelect.options].some((o) => o.value === "rule34")) {
    dom.sourceSelect.value = "rule34";
  }
}

dom.searchBtn.addEventListener("click", () => startSearch());
dom.loadMoreBtn.addEventListener("click", () => {
  loadNextPage(true);
});
dom.favoritesBtn.addEventListener("click", () => {
  if (state.viewMode === "favorites") {
    showSearchResults();
  } else {
    showFavorites();
  }
});

dom.blacklistInput.addEventListener("input", () => {
  savePreferences();
  renderGrid();
});
dom.tagsInput.addEventListener("input", () => {
  savePreferences();
  queueTagHints();
});
dom.tagsInput.addEventListener("focus", queueTagHints);
dom.tagsInput.addEventListener("blur", () => {
  const normalizedTags = normalizeTagInputDisplay(dom.tagsInput.value);
  if (dom.tagsInput.value !== normalizedTags) {
    dom.tagsInput.value = normalizedTags;
    savePreferences();
  }
});
dom.tagsInput.addEventListener("keydown", (event) => {
  if (event.key === "Escape") hideHints();
});

dom.tagHints.addEventListener("click", (event) => {
  const target = event.target.closest(".hint-item");
  if (!target) return;
  applyTagHint(target.dataset.tag || "");
});

dom.paginationBar.addEventListener("click", (event) => {
  const pageTarget = event.target.closest("[data-page]");
  if (pageTarget) {
    const page = Number(pageTarget.dataset.page);
    if (!Number.isNaN(page)) {
      state.currentPage = page;
      renderGrid();
      renderPagination();
    }
    return;
  }
  const loadTarget = event.target.closest("[data-load-next]");
  if (loadTarget) {
    loadNextPage(true);
  }
});

dom.grid.addEventListener("click", (event) => {
  const dlBtn = event.target.closest("[data-download-index]");
  if (dlBtn) {
    const idx = Number(dlBtn.dataset.downloadIndex);
    if (!Number.isNaN(idx) && state.filteredItems[idx]) {
      downloadItem(state.filteredItems[idx]);
    }
    return;
  }
  const likeBtn = event.target.closest("[data-like-index]");
  if (likeBtn) {
    const idx = Number(likeBtn.dataset.likeIndex);
    if (!Number.isNaN(idx) && state.filteredItems[idx]) {
      toggleFavorite(state.filteredItems[idx]);
    }
    return;
  }
  if (event.target.closest("[data-stop-open]")) {
    return;
  }
  const card = event.target.closest(".card");
  if (!card) return;
  openViewer(Number(card.dataset.index));
});

dom.grid.addEventListener(
  "error",
  (event) => {
    const media = event.target;
    if (!(media instanceof HTMLImageElement || media instanceof HTMLVideoElement)) return;
    const directSrc = media.dataset.directSrc || "";
    if (directSrc && media.src !== directSrc) {
      media.src = directSrc;
    }
  },
  true
);

dom.closeBtn.addEventListener("click", closeViewer);
dom.prevBtn.addEventListener("click", () => nextViewer(-1));
dom.nextBtn.addEventListener("click", () => nextViewer(1));
dom.viewer.addEventListener("close", () => {
  stopViewerMedia();
  state.viewerIndex = -1;
  dom.likeCurrentBtn.classList.remove("toggle-active");
  dom.likeCurrentBtn.classList.add("hidden");
});
dom.downloadCurrentBtn.addEventListener("click", () => {
  if (state.viewerIndex < 0) return;
  const item = state.filteredItems[state.viewerIndex];
  if (item) downloadItem(item);
});
dom.likeCurrentBtn.addEventListener("click", () => {
  if (state.viewerIndex < 0) return;
  const item = state.filteredItems[state.viewerIndex];
  if (item) toggleFavorite(item);
});
dom.openDownloadsBtn.addEventListener("click", async () => {
  try {
    const res = await fetch("/api/downloads/open", { method: "POST" });
    const data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error(data.error || "Не удалось открыть папку загрузок");
    }
    setStatus(`Папка: ${data.path}`);
  } catch (error) {
    setError(error.message || String(error));
  }
});

[
  dom.rule34UserId,
  dom.rule34ApiKey,
  dom.gelbooruUserId,
  dom.gelbooruApiKey,
  dom.proxyUrl,
].forEach((el) => {
  el.addEventListener("input", () => {
    queueSecureSave();
    queueTagHints();
  });
});

[dom.sortSelect, dom.limitSelect, dom.minScoreSelect, dom.sourceSelect, dom.adultCheckbox].forEach((el) => {
  el.addEventListener("change", () => {
    savePreferences();
  });
});

dom.themeSelect.addEventListener("change", () => {
  applyTheme(dom.themeSelect.value);
  savePreferences();
  renderGrid();
});

dom.clearSecureBtn.addEventListener("click", async () => {
  try {
    await fetch("/api/secure-config/clear", { method: "POST" });
    applySecurePayload({});
    refreshSecurityStatus();
    setStatus("Secure-хранилище очищено");
  } catch {
    setError("Не удалось очистить secure-хранилище");
  }
});

window.addEventListener("click", (event) => {
  if (event.target === dom.tagsInput || dom.tagHints.contains(event.target)) return;
  hideHints();
});

window.addEventListener("keydown", (event) => {
  if (!dom.viewer.open) return;
  if (event.key === "Escape") closeViewer();
  if (event.key === "ArrowLeft") nextViewer(-1);
  if (event.key === "ArrowRight") nextViewer(1);
});

async function bootstrap() {
  loadPreferences();
  loadFavorites();
  document.body.setAttribute("data-platform", state.performanceMode ? "android" : "desktop");
  applyTheme(dom.themeSelect.value || "old_neko");
  updateFavoritesButton();
  dom.likeCurrentBtn.classList.add("hidden");
  if (state.performanceMode && !localStorage.getItem("bf_limitSelect")) {
    dom.limitSelect.value = "20";
  }
  dom.minScoreSelect.value = "0";
  await fetchSources();
  await loadSecureConfig();
  await refreshSecurityStatus();
  setupAutoPaging();
  setStatus("Готово к поиску");
}

bootstrap();
