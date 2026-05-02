(function () {
  var script = document.currentScript || (function () {
    var scripts = document.getElementsByTagName('script');
    return scripts[scripts.length - 1];
  })();

  var channelId = script.getAttribute('data-channel-id') || '';

  // Capture UTM params + fbclid from URL
  var params = new URLSearchParams(window.location.search);
  var utms = {};
  ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(function (k) {
    var v = params.get(k);
    if (v) utms[k] = v;
  });

  // Capture fbclid → format Meta fbc (fb.1.<timestamp>.<fbclid>)
  var fbclid = params.get('fbclid');
  if (fbclid) {
    var fbc = 'fb.1.' + Date.now() + '.' + fbclid;
    localStorage.setItem('_trk_fbc', fbc);
    document.cookie = '_fbc=' + fbc + '; path=/; max-age=15552000'; // 180 dias
  }

  // Lê _fbp do cookie (definido pelo Pixel JS) ou gera um novo
  function getFbp() {
    var m = document.cookie.match(/(?:^|; )_fbp=([^;]+)/);
    if (m) return m[1];
    var stored = localStorage.getItem('_trk_fbp');
    if (stored) return stored;
    var fbp = 'fb.1.' + Date.now() + '.' + Math.floor(Math.random() * 1e10);
    localStorage.setItem('_trk_fbp', fbp);
    return fbp;
  }
  function getFbc() {
    var m = document.cookie.match(/(?:^|; )_fbc=([^;]+)/);
    if (m) return m[1];
    return localStorage.getItem('_trk_fbc') || '';
  }

  // Merge with stored UTMs (don't overwrite if already set from a previous visit)
  var stored = {};
  try { stored = JSON.parse(localStorage.getItem('_trk_utms') || '{}'); } catch (e) {}
  var merged = Object.assign({}, stored, utms);
  if (Object.keys(merged).length) {
    localStorage.setItem('_trk_utms', JSON.stringify(merged));
  }

  if (channelId) localStorage.setItem('_trk_channel', channelId);

  // External ID estável (anônimo) por device — usado pra match quality
  var extId = localStorage.getItem('_trk_eid');
  if (!extId) {
    extId = 'eid_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 10);
    localStorage.setItem('_trk_eid', extId);
  }

  function buildPayload(extra) {
    var savedUtms = {};
    try { savedUtms = JSON.parse(localStorage.getItem('_trk_utms') || '{}'); } catch (ex) {}
    var base = {
      channel_id: channelId,
      page_url: location.href,
      referrer: document.referrer || '',
      fbc: getFbc(),
      fbp: getFbp(),
      external_id: extId,
      user_agent: navigator.userAgent,
    };
    return Object.assign(base, savedUtms, extra || {});
  }

  // Send pageview on page load
  try {
    var pageviewBlob = new Blob([JSON.stringify(buildPayload())], { type: 'text/plain' });
    navigator.sendBeacon(script.src.replace('/static/tracker.js', '/tracker/pageview'), pageviewBlob);
  } catch (ex) {}

  // Intercept clicks on Telegram links — attach attribution data
  var beaconBase = script.src.replace('/static/tracker.js', '');

  function isTelegramHref(href) {
    return href && (href.indexOf('t.me') !== -1 || href.indexOf('telegram.me') !== -1 || href.indexOf('telegram.dog') !== -1);
  }

  function sendEntrada() {
    try {
      var blob = new Blob([JSON.stringify(buildPayload())], { type: 'text/plain' });
      navigator.sendBeacon(beaconBase + '/tracker/entrada', blob);
    } catch (ex) {}
  }

  document.addEventListener('click', function (e) {
    // Sobe na árvore DOM para encontrar o <a> mesmo que o clique seja num filho
    var el = e.target;
    while (el && el.tagName !== 'A') el = el.parentElement;
    if (el && el.tagName === 'A' && isTelegramHref(el.href)) {
      sendEntrada();
      return;
    }
    // Detecta botões/divs que redirecionam para Telegram via onclick ou data-href
    var btn = e.target.closest('[data-href],[onclick]');
    if (btn) {
      var dh = btn.getAttribute('data-href') || '';
      if (isTelegramHref(dh)) { sendEntrada(); return; }
      var oc = btn.getAttribute('onclick') || '';
      if (isTelegramHref(oc)) { sendEntrada(); return; }
    }
  });
})();
