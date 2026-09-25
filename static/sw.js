// Service worker do VS Track: recebe notificações push e abre o painel ao tocar.
// Não faz cache de páginas — o painel sempre carrega a versão mais nova.
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

self.addEventListener("push", (event) => {
  let d = {};
  try { d = event.data ? event.data.json() : {}; } catch (e) { d = { corpo: event.data && event.data.text() }; }
  event.waitUntil(self.registration.showNotification(d.titulo || "VS Track", {
    body: d.corpo || "",
    icon: "/static/icon-192.png",
    badge: "/static/badge-96.png",
    tag: d.tipo === "teste" ? "teste" : undefined,
    data: { url: d.url || "/static/dashboard.html" },
  }));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url) || "/static/dashboard.html";
  event.waitUntil((async () => {
    const abertas = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
    for (const c of abertas) {
      if (c.url.includes("/static/dashboard.html")) { await c.focus(); return; }
    }
    await self.clients.openWindow(url);
  })());
});
