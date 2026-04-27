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

