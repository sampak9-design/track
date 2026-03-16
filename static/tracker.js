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
  document.addEventListener('click', function (e) {
    var link = e.target.closest('a[href*="t.me"]');
    if (!link) return;

    var savedUtms = {};
    try { savedUtms = JSON.parse(localStorage.getItem('_trk_utms') || '{}'); } catch (ex) {}

    // Send entrada event with UTMs to tracker backend
    var payload = Object.assign({ channel_id: channelId, page_url: location.href }, savedUtms);
    try {
      var blob = new Blob([JSON.stringify(payload)], { type: 'application/json' });
      navigator.sendBeacon(script.src.replace('/static/tracker.js', '/tracker/entrada'), blob);
    } catch (ex) {}
  });
})();
