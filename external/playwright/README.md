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
| `playwright.page.actions.config.file` | _unset_ | Path to a JSON file declaring an ordered chain of `PageAction` implementations applied after `page.navigate()` succeeds and before `page.content()` is captured. Use this to plug site-specific post-navigate behaviour (tab/accordion expansion, cookie-banner dismissal, scroll-to-bottom, custom `evaluate()` calls, ...) into the protocol without subclassing it. The chain runs only when content would otherwise be captured (i.e. on 2xx, or on non-2xx if `playwright.capture.content.on.error` is `true`). |

Per-URL metadata triggers:

| Metadata key | Effect |
|---|---|
| `playwright.trace` | If present on the input metadata, a Playwright trace zip is recorded for the navigation and its path is returned in the response metadata under the same key. |

## Page actions

Custom post-navigate behaviour is added by implementing `PageAction` and listing the implementation in the JSON file referenced by `playwright.page.actions.config.file`. Actions follow the same `Configurable` pattern as URL/parse filters and are loaded as an ordered chain. A failure in one action is logged and swallowed so the rest of the chain still runs.

```json
{
  "org.apache.stormcrawler.protocol.playwright.PageActions": [
    {
      "class": "org.apache.stormcrawler.protocol.playwright.actions.ExpandClickablesAction",
      "name": "tabs",
      "params": {
        "selectors": [".tab-widget .tab-header"],
        "root": ".tab-widget",
        "body": ".tab-widget-body",
        "waitMs": 300
      }
    }
  ]
}
```

### Built-in actions

| Class | Purpose |
|---|---|
| `ExpandClickablesAction` | Clicks every element matching the configured selectors and clones the resulting body container into a hidden cache under the same widget root, so `page.content()` ends up containing the HTML of every tab/accordion panel rather than only the originally active one. Anchors with an `href` are skipped. Parameters: `selectors` (array, required), `root` (string, required), `body` (string, required), `waitMs` (int, default `200`), `clickTimeoutMs` (int, default `2000`). |
| `EvaluateAction` | Evaluates a list of JavaScript expressions on the page and stores the JSON-serialised result of each in the response metadata. Parameters: `expressions` (array of strings, required), `keyPrefix` (string, optional — when set, results are stored under `keyPrefix + index` rather than under the expression itself, matching the legacy `playwright.evaluations` behaviour). |
| `ScrollToBottomAction` | Repeatedly scrolls to the bottom of the page until the document height stops growing, the step cap is reached, or the time budget elapses — useful for infinite-scroll feeds. Parameters: `waitMs` (int, default `500`), `maxSteps` (int, default `20`), `maxDurationMs` (int, default `15000`). |
| `WaitForSelectorAction` | Waits for a selector to reach a given state before allowing the chain to proceed. By default a timeout is treated as a soft failure (logged and swallowed); set `required: true` to fail the action on timeout. Parameters: `selector` (string, required), `state` (one of `attached`, `detached`, `visible`, `hidden` — default `visible`), `timeoutMs` (int, default `5000`), `required` (bool, default `false`). |
| `DismissOverlayAction` | Dismisses cookie banners, GDPR walls, newsletter modals, etc. by clicking the first match of each selector, and optionally removes sticky overlays by deleting matching elements from the DOM. Missing elements and click failures are silently skipped. Parameters: `selectors` (array of strings), `removeSelectors` (array of strings), `timeoutMs` (int, default `1500`). At least one of `selectors` or `removeSelectors` must be non-empty. |
| `ScreenshotAction` | Captures a screenshot of the page and stores it base64-encoded in the response metadata. Intended for diagnostics and small-volume use; larger crawls should write to a blob store instead. Parameters: `metadataKey` (string, default `playwright.screenshot`), `fullPage` (bool, default `false`), `type` (`png` or `jpeg`, default `png`), `quality` (int 0-100, only honoured for JPEG). |

### Writing your own action

Extend `PageAction` (which extends `AbstractConfigurable`) and implement `apply(Page, url, sourceMetadata, responseMetadata)`. Read parameters from the supplied `JsonNode` in `configure()` and validate at load time — anything thrown there propagates through `PageActions.fromConf()` and stops the topology from starting with a misconfigured chain.

```java
public class MyAction extends PageAction {

    private String selector;

    @Override
    public void configure(final Map<String, Object> stormConf, final JsonNode params) {
        if (!params.has("selector")) {
            throw new IllegalArgumentException("MyAction requires 'selector'");
        }
        this.selector = params.get("selector").asText();
    }

    @Override
    public void apply(final Page page, final String url,
                      final Metadata sourceMetadata, final Metadata responseMetadata) {
        page.locator(selector).click();
    }
}
```

Reference it from the chain JSON via its fully-qualified class name. Per-action failures are logged and swallowed by the chain wrapper so one bad action cannot abort the rest — if you need a hard failure, raise it from `configure()`, not from `apply()`.

## Tests

The module has three test classes:

- `PageActionsTest` — JSON loader / chain construction (no browser).
- `actions/ActionConfigureTest` — `configure()` validation for every built-in (no browser).
- `PageActionsLiveTest` — end-to-end browser tests for the chain and individual actions.

The live tests use the same `assumeTrue("false".equals(System.getProperty("CI_ENV", "false")))` gate as `ProtocolTest`, so they run locally (`mvn test`) but skip on CI runners launched with `-DCI_ENV=true`. They expect a usable Chromium — either via `mvn exec:java -e -Dexec.mainClass=com.microsoft.playwright.CLI -Dexec.args="install chromium"` or by pointing `playwright.cdp.url` at an existing browser.

