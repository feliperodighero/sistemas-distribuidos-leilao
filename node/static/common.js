// Funções compartilhadas pelo cliente e pelo painel.
// O ponto principal é o FAILOVER: se o nó em uso não responde, o cliente
// passa a usar outro nó do cluster (a lista vem de /api/cluster).

const Cluster = {
  nodes: [location.origin],   // URLs públicas conhecidas
  current: 0,
  onSwitch: null,

  get base() { return this.nodes[this.current]; },

  async fetch(path, opts = {}, timeoutMs = 4000) {
    let lastErr;
    for (let attempt = 0; attempt < this.nodes.length; attempt++) {
      const base = this.base;
      try {
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), timeoutMs);
        const resp = await fetch(base + path, { ...opts, signal: ctrl.signal });
        clearTimeout(timer);
        // nó "derrubado" pelo modo apresentação: responde 503 com este cabeçalho
        if (resp.status === 503 && resp.headers.get("X-Node-Crashed")) throw new Error("nó fora do ar");
        return resp;
      } catch (err) {
        lastErr = err;
        this.current = (this.current + 1) % this.nodes.length;
        if (this.onSwitch) this.onSwitch(base, this.base);
      }
    }
    throw lastErr || new Error("nenhum nó respondeu");
  },

  async json(path, opts, timeoutMs) {
    const resp = await this.fetch(path, opts, timeoutMs);
    const body = await resp.json().catch(() => ({}));
    return { status: resp.status, body };
  },

  // Atualiza a lista de nós a partir da visão do nó atual.
  async refresh() {
    const { body } = await this.json("/api/cluster");
    const base = this.base;
    const urls = (body.nodes || []).map(n => n.public_url).filter(Boolean);
    if (urls.length) {
      this.nodes = [...new Set([base, ...urls])];
      this.current = 0;
    }
    return body;
  },
};

function nodeTag(id) {
  return `<span class="node-tag c${((id - 1) % 5) + 1}">nó ${id}</span>`;
}

function brl(v) {
  return (v ?? 0).toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function toast(msg, kind = "") {
  const box = document.getElementById("toasts");
  if (!box) return;
  const el = document.createElement("div");
  el.className = "toast " + kind;
  el.textContent = msg;
  box.appendChild(el);
  setTimeout(() => el.remove(), 5000);
}

function store(key, value) {
  try {
    if (value === undefined) return localStorage.getItem(key);
    localStorage.setItem(key, value);
  } catch { return null; }
}

function nodeIdOf(url) {
  const m = /:(\d+)\/?$/.exec(url);
  return m ? m[1] : url;
}
