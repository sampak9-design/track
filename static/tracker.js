(function () {
  var script = document.currentScript || (function () {
    var scripts = document.getElementsByTagName('script');
    return scripts[scripts.length - 1];
  })();

  var channelId = script.getAttribute('data-channel-id') || '';

  // Capture UTM params from URL
  var params = new URLSearchParams(window.location.search);
  var utms = {};
  ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(function (k) {
    var v = params.get(k);
    if (v) utms[k] = v;
  });

  // Merge with stored UTMs (don't overwrite if already set from a previous visit)
  var stored = {};
  try { stored = JSON.parse(localStorage.getItem('_trk_utms') || '{}'); } catch (e) {}
  var merged = Object.assign({}, stored, utms);
  if (Object.keys(merged).length) {
    localStorage.setItem('_trk_utms', JSON.stringify(merged));
  }

  // Store channel info
  if (channelId) localStorage.setItem('_trk_channel', channelId);

  // Send pageview on page load
  var savedUtmsForPageview = {};
  try { savedUtmsForPageview = JSON.parse(localStorage.getItem('_trk_utms') || '{}'); } catch (ex) {}
  var pageviewPayload = Object.assign({ channel_id: channelId, page_url: location.href }, savedUtmsForPageview);
  try {
    var pageviewBlob = new Blob([JSON.stringify(pageviewPayload)], { type: 'application/json' });
    navigator.sendBeacon(script.src.replace('/static/tracker.js', '/tracker/pageview'), pageviewBlob);
  } catch (ex) {}

  // Intercept clicks on Telegram links — attach UTMs to server
  var beaconBase = script.src.replace('/static/tracker.js', '');

  function isTelegramHref(href) {
    return href && (href.indexOf('t.me') !== -1 || href.indexOf('telegram.me') !== -1 || href.indexOf('telegram.dog') !== -1);
  }

  function sendEntrada() {
    var savedUtms = {};
    try { savedUtms = JSON.parse(localStorage.getItem('_trk_utms') || '{}'); } catch (ex) {}
    var payload = Object.assign({ channel_id: channelId, page_url: location.href }, savedUtms);
    try {
      var blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
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
