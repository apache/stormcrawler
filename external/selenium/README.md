# Selenium Protocol for Apache StormCrawler

This project provides a **[Selenium](https://www.selenium.dev/)-based protocol implementation** for Apache StormCrawler.

---

## Overview

The [Selenium](https://www.selenium.dev) protocol allows StormCrawler to interact with dynamic web pages using Selenium WebDriver. It is particularly useful for crawling JavaScript-heavy sites that require a real browser environment.

---

## Configuration

Add `selenium-conf.yaml` to your topology configuration. Below is a sample configuration:

```yaml
# navigationfilters.config.file: "navigationfilters.json"
# selenium.addresses: "http://localhost:9515"

# Enable or disable Selenium tracing (default: false)
selenium.tracing: false

# Selenium timeouts (rely on Selenium defaults if set to -1)
selenium.timeouts:
  script: -1       # Maximum time for scripts to run
  pageLoad: -1     # Maximum time to wait for page load
  implicit: -1     # Implicit wait time for finding elements

# Selenium capabilities
# selenium.capabilities:
#   browserName: "chrome"  # Required: choose your browser
#   phantomjs.page.settings.userAgent: "$userAgent"  # Example: set custom user agent
#   
#   # ChromeDriver specific options
#   goog:chromeOptions:
#     args:
#       - "--headless"       # Run Chrome in headless mode
#       - "--disable-gpu"    # Disable GPU acceleration
#       - "--mute-audio"     # Mute audio output



