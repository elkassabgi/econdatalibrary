using Workerd = import "/workerd/workerd.capnp";
const config :Workerd.Config = (
  services = [
    (name = "first", worker = .first),
    (name = "second", worker = .second),
    (name = "loop", network = (allow = ["local"])),
  ],
  sockets = [
    (name = "a", address = "127.0.0.1:18820", http = (), service = "first"),
    (name = "b", address = "127.0.0.1:18822", http = (), service = "second"),
  ],
);
const first :Workerd.Worker = (
  modules = [ (name = "worker.mjs", esModule = embed "worker.mjs") ],
  compatibilityDate = "2024-09-23",
  bindings = [ (name = "ROLE", text = "first") ],
  globalOutbound = "loop",
);
const second :Workerd.Worker = (
  modules = [ (name = "worker.mjs", esModule = embed "worker.mjs") ],
  compatibilityDate = "2024-09-23",
  bindings = [ (name = "ROLE", text = "second") ],
);
