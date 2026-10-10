// Toy worker of tools/selfhost/path_fuzz: it echoes how workerd reads a request target, and makes one real
// HTTP hop the way wrangler's ProxyWorker does (new URL(request.url), host replaced, fetch) so that the
// second reading is reported too. Written in review AR-268.
function read(request) {
  const h = {};
  for (const [k, v] of request.headers) h[k] = v;
  let pathname = null, search = null, hash = null, err = null;
  try { const u = new URL(request.url); pathname = u.pathname; search = u.search; hash = u.hash; } catch (e) { err = String(e); }
  return { method: request.method, raw: request.url, pathname, search, hash, err, headers: h };
}
export default {
  async fetch(request, env) {
    const me = read(request);
    if (env.ROLE === "second") return Response.json(me);
    let hop = null;
    if (me.pathname !== null) {
      const u = new URL(request.url);
      u.protocol = "http:"; u.hostname = "127.0.0.1"; u.port = "18822";
      try { const r = await fetch(u, { headers: request.headers }); hop = await r.json(); } catch (e) { hop = { err: String(e) }; }
    }
    return Response.json({ first: me, second: hop });
  },
};
