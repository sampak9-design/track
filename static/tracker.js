(function () {
  var script = document.currentScript || (function () {
    var scripts = document.getElementsByTagName('script');
    return scripts[scripts.length - 1];
  })();

  var channelId = script.getAttribute('data-channel-id') || '';
  var channelUsername = script.getAttribute('data-channel-username') || '';

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

  // Intercept clicks on Telegram links — attach UTMs to server
  document.addEventListener('click', function (e) {
    var link = e.target.closest('a[href*="t.me"]');
    if (!link || !channelId) return;

    var savedUtms = {};
    try { savedUtms = JSON.parse(localStorage.getItem('_trk_utms') || '{}'); } catch (ex) {}

    // Send click event with UTMs to tracker backend
    var payload = Object.assign({ channel_id: channelId }, savedUtms);
    try {
      navigator.sendBeacon(script.src.replace('/static/tracker.js', '/tracker/click'), JSON.stringify(payload));
    } catch (ex) {}
  });
})();
