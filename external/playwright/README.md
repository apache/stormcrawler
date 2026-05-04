# Playwright
Protocol implementation for Apache StormCrawler based on Playwright

## Standalone Chrome

Playwright installs the browsers it needs locally, which can take more than 1 GB disk space and takes some time. Instead, you can install Chrome separately and get Playwright to connect to its CDP.

[Chrome for Testing](https://github.com/GoogleChromeLabs/chrome-for-testing) is the recommended way of installing a standalone version of Chrome. 

You can start it like so

```
 ./chrome --remote-debugging-port=9222 --user-data-dir=remote-profile --headless &
```
NOTE this is how ChromeDriver launches Chrome

```
chrome --allow-pre-commit-input --disable-background-networking --disable-client-side-phishing-detection --disable-default-apps --disable-dev-shm-usage --disable-gpu --disable-hang-monitor --disable-popup-blocking --disable-prompt-on-repost --disable-software-rasterizer --disable-sync --dns-prefetch-disable --enable-automation --enable-logging --headless --log-level=0 --mute-audio --no-first-run --no-sandbox --no-service-autorun --password-store=basic --remote-debugging-port=9222 --use-mock-keychain --user-data-dir=/tmp/.org.chromium.Chromium.CPTbw3 data:,
```

Then URL for the CDP connection is configured with _playwright.cdp.url_.

Alternatively, you can still rely on what Playwright installs but do it manually and just for the browser type you want

```
mvn exec:java -e -Dexec.mainClass=com.microsoft.playwright.CLI -Dexec.args="install chromium"
```

The setting `playwright.skip.download` to `true` in the configuration will assume that the browser has been installed and will not trigger the installation of all the different browsers.

## Configuration

| Property | Default | Description |
|---|---|---|
| `playwright.cdp.url` | _unset_ | If set, connect to an existing Chrome instance via the Chrome DevTools Protocol (e.g. `http://localhost:9222`) instead of launching a browser. |
| `playwright.remote.ws` | _unset_ | If set, connect to a remote Playwright server over WebSocket (e.g. `ws://localhost:3000/`). Mutually exclusive with `playwright.cdp.url`. |
| `playwright.skip.download` | `false` | If `true`, sets `PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=true` so Playwright will not install browsers. Implicitly forced to `true` when `playwright.cdp.url` or `playwright.remote.ws` is set. |
| `playwright.load.event` | `load` | The Playwright `WaitUntilState` to wait for before considering the page ready. Accepts `load`, `domcontentloaded`, or `networkidle`. |
| `playwright.skip.resource.types` | _empty_ | List of resource types to abort during navigation (`document`, `stylesheet`, `image`, `media`, `font`, `script`, `texttrack`, `xhr`, `fetch`, `eventsource`, `websocket`, `manifest`, `other`). |
| `playwright.evaluations` | _empty_ | List of JavaScript expressions evaluated on the page after load. Each result is JSON-serialized and stored in the response metadata under the expression itself as the key. |
| `playwright.capture.content.on.error` | `false` | By default the rendered DOM is only captured when the origin returns a 2xx status. Set to `true` to also capture `page.content()` for non-2xx responses — useful for Single-Page Applications that return a non-2xx stub document and then hydrate the real content via JavaScript. |
| `playwright.override.status.on.content` | `false` | When the rendered DOM was captured for a non-2xx response, override the reported HTTP status with `200` so downstream components treat the URL as `FETCHED`. The original origin status is preserved in the response metadata under the key `playwright.origin.status`. No-op unless `playwright.capture.content.on.error` is also `true`. |

Per-URL metadata triggers:

| Metadata key | Effect |
|---|---|
| `playwright.trace` | If present on the input metadata, a Playwright trace zip is recorded for the navigation and its path is returned in the response metadata under the same key. |

## JS rendering detection

Browser-based fetching is expensive — typically 10–50× slower than a plain HTTP fetch and limited by how many browsers a host can run concurrently. Most operators only want Playwright on the URLs that actually need it. The `JsRenderingDetector` parse filter solves the routing question without adding new infrastructure: it inspects the parsed page from a cheap fetch and, when the content looks JS-rendered, sets a metadata flag that `DelegatorProtocol` (already part of `core`) routes on.

### How detection works

The filter applies four heuristics, cheapest-first, and short-circuits on the first hit:

1. **SPA framework fingerprints** in raw HTML — `data-reactroot`, `ng-version=`, `__NEXT_DATA__`, `window.__NUXT__`, `data-svelte-h=`, `data-vue-app`, `data-astro-cid`, `<router-outlet`. Defaults are overridable via the `fingerprints` parameter.
2. **`<noscript>` blocks** that explicitly request JavaScript — match patterns like _"enable JavaScript"_, _"requires JavaScript"_, _"JavaScript is disabled"_.
3. **Empty SPA hydration roots** — `<div id="root"></div>` / `#app` / `#__next` / `#__nuxt` with no children. IDs override­able via `emptyRootIds`.
4. **Outcome-based fallback** — when at least one `<script>` is present and both `text.length < minTextLength` (default 200) and `outlinks.size() < minOutlinks` (default 2), the URL is flagged as a thin SPA. The `<script>` gate keeps the filter from flagging static error stubs.

### What the filter sets

| Metadata key | Value | Notes |
|---|---|---|
| `fetch.with` | `playwright` | Routing key, override­able via `metadataKey` / `metadataValue`. |
| `fetch.with.reason` | e.g. `fingerprint:data-reactroot`, `noscript-js-required`, `empty-root:root`, `thin-content:text=12,outlinks=0` | Diagnostic — set unless `recordReason: false`. |

### Loop guards

- Detection is skipped when `playwright.protocol.end` is already present on the URL — i.e. the URL was just fetched by Playwright; reapplying the heuristic would just reflag it. Override the watch key via `skipIfMetadataPresent`.
- Detection is also skipped when the routing key is already set, so the filter is idempotent and safe to leave permanently in `parsefilters.json`.

### Parameters

| Name | Type | Default | Notes |
|---|---|---|---|
| `metadataKey` | string | `fetch.with` | Routing key set on a hit. |
| `metadataValue` | string | `playwright` | Value to set. |
| `minTextLength` | int | `200` | Outcome-based threshold for visible text. |
| `minOutlinks` | int | `2` | Outcome-based threshold for extracted outlinks. |
| `fingerprints` | string array | _see above_ | Substrings searched in raw HTML; replaces defaults when set. |
| `emptyRootIds` | string array | `["root","app","__next","__nuxt"]` | Element IDs treated as empty SPA hydration roots. |
| `requiredMessages` | string array | _empty_ | Additional substrings that, when found anywhere in the HTML, flag the URL. Use for site-specific JS-required prompts and loader text that don't fit the noscript pattern (e.g. `"Loading..."`, `"[object Object]"`, `"Please enable cookies"`). |
| `skipIfMetadataPresent` | string | `playwright.protocol.end` | Short-circuit when this metadata key is set. Empty string disables. |
| `recordReason` | bool | `true` | Also set `metadataKey + ".reason"` describing which signal fired. |

### Wiring

Add the filter to your `parsefilters.json`:

```json
{
  "class": "org.apache.stormcrawler.protocol.playwright.parsefilter.JsRenderingDetector",
  "name": "js-rendering-detector",
  "params": { "minTextLength": 200, "minOutlinks": 2 }
}
```

Route on the metadata key it sets via `DelegatorProtocol`:

```yaml
http.protocol.implementation:  "org.apache.stormcrawler.protocol.DelegatorProtocol"
https.protocol.implementation: "org.apache.stormcrawler.protocol.DelegatorProtocol"
protocol.delegator.config:
  - className: "org.apache.stormcrawler.protocol.playwright.HttpProtocol"
    filters:
      "fetch.with": "playwright"
  - className: "org.apache.stormcrawler.protocol.okhttp.HttpProtocol"
```

A few wiring notes:

- The dotted metadata key (`fetch.with`) is quoted in the YAML above to make it unambiguous to a human reader; SnakeYAML treats unquoted `fetch.with: "playwright"` as the same single-key scalar, so either parses correctly.
- `DelegatorProtocol` requires the **last** entry in `protocol.delegator.config` to have no `filters:` — it acts as the fallback. Keep OkHttp (or whichever cheap protocol you pick) at the bottom of the list.
- The filter alone does **not** trigger an immediate refetch. It only sets the metadata; the URL is rescheduled by `DefaultScheduler` according to the FETCHED interval (`fetchInterval.default`, 24h by default), and `DelegatorProtocol` picks Playwright on the next scheduled fetch. To get faster turnaround, either drop in the `JsRenderingRedirectionBolt` described below, or add a per-metadata-key fetch interval to your YAML: `fetchInterval.fetch.with=playwright: 5` (refetch flagged URLs in 5 minutes instead of 24 hours).
- Sibling URLs on the same host don't inherit the flag — that requires a host-keyed metadata transfer scheme and is intentionally out of scope.

### Forcing an immediate refetch — `JsRenderingRedirectionBolt`

The detector flags URLs but doesn't, on its own, prevent the cheap fetch's stub document from flowing downstream into the parser, indexer, and outlink emission. For most crawls that's fine — the next scheduled fetch replaces the stub with the rendered version. If you want the stub to be discarded and the URL refetched immediately through Playwright, drop `JsRenderingRedirectionBolt` between the parser and the indexer. The bolt:

- reads the routing flag set by the detector (or any other upstream component),
- on hit, emits **only** to `StatusStreamName` with `Status.FETCHED` so the URL is rescheduled and the stub never reaches the index,
- on miss, passes the tuple through unchanged,
- short-circuits when `playwright.protocol.end` is already on the URL — the loop guard.

The bolt has no detection logic of its own; it just acts on the metadata flag. That keeps the heuristics in one place (the parse filter) and lets you swap or extend the bolt independently.

Topology fragment:

```text
... -> JSoupParserBolt -> JsRenderingRedirectionBolt -> IndexerBolt -> ...
                                                    \-> StatusStream
```

YAML:

```yaml
# refetch flagged URLs in 5 minutes rather than 24 hours
fetchInterval.fetch.with=playwright: 5
```

Configuration keys:

| Key | Default | Notes |
|---|---|---|
| `playwright.redirect.metadata.key` | `fetch.with` | Routing key the bolt watches for. |
| `playwright.redirect.metadata.value` | `playwright` | Value the bolt watches for. |
| `playwright.redirect.skip.if.metadata.present` | `playwright.protocol.end` | Loop guard — pass through unchanged when this key is set on the URL. Empty string disables. |

### When _not_ to use it

- **Operator allowlist suffices.** If you already know which hosts need a browser, add them as a `urlPatterns` rule on the Playwright leg of `DelegatorProtocol` and skip the filter.
- **Anti-bot / WAF challenge pages.** Cloudflare, DataDome, and Akamai challenge fingerprints aren't covered here; those usually need a stealth-mode browser, not just rendering.
- **Aggressively first-fetch-sensitive crawls.** The first fetch on an unknown SPA host is always wasted (you get a stub document) before the filter learns about the host. If that's unacceptable, prefer the operator allowlist.

