# stormcrawler-ai
================================

This module contains StormCrawler-compatible content extraction components that use a Large Language Model (LLM) via an OpenAI-compatible API to extract meaningful text from HTML documents.
This enables context-aware and semantically rich extraction beyond traditional rule-based approaches.

## Prerequisites

Add `stormcrawler-ai` to the dependencies of your project\:

```xml
<dependency>
    <groupId>org.apache.stormcrawler</groupId>
    <artifactId>stormcrawler-ai</artifactId>
    <version>XXXX</version>
</dependency>
```
In addition, you either need access to an OpenAI-API compatible model serving hoster or provide an inference service yourself.

## Features

- Uses OpenAI-compatible LLMs (e.g., LLaMA 3) for intelligent HTML parsing and content extraction.
- Customizable prompts for both system and user messages.
- Easily integrates with StormCrawler parsing pipelines.
- Optional listener interface for logging or usage metrics.

## Configuration

To use the `LlmTextExtractor`, your configuration file must include the following:

```yaml
# Required: specify the extractor class
textextractor.class: "org.apache.stormcrawler.ai.OpenAITextExtractor"

# Required: LLM API settings
textextractor.llm.api_key: "<your-api-key>"
textextractor.llm.url: "https://<your-openai-compatible-endpoint>"
textextractor.llm.model: "<your-model-to-use>"

# Optional: system prompt sent to LLM
textextractor.system.prompt: "You are an expert in extracting content from plain HTML input."

# Optional: user prompt template (with placeholders) for custom use cases. Note: We provide a default prompt in `src/main/resources/llm-default-prompt.txt`
textextractor.llm.prompt: |
  Please extract the main textual content from the following HTML:
  {HTML}

  {REQUEST}

# Optional: extra request passed into the user prompt
textextractor.llm.user_request: "Only include body content relevant to articles."

# Optional: maximum number of characters of extracted text, -1 (default) for no limit
textextractor.llm.text.maxlength: 100000

# Optional: listener class implementing LlmResponseListener to hook into success/failure of LLM response, i.e. for tracking usage metrics.
textextractor.llm.listener.clazz: "<your-listener-class>"
```

Marker tokens of the form `<|NAME|>` that appear in the prompt template, such as `<|HTML_CONTENT_END|>` in the default one, are removed from the page HTML before it is substituted, so that a page cannot open or close a section of the prompt. If your own template delimits its sections, use markers of that form.

The text returned is the content of the `<content>…</content>` envelope that the default prompt asks for, or the whole reply if it has none. Any HTML markup left in it is removed, so the result contains only text, as with the default `TextExtractor`.

Note: You **must** set `textextractor.class` to use this extractor in a StormCrawler topology. 

The `LlmTextExtractor` does not support the following configuration options from the default `TextExtractor`:

- `textextractor.include.pattern`
- `textextractor.exclude.tags`
- `textextractor.no.text`
- `textextractor.skip.after`

## Additional Notes
- **LLM Costs:** Calls to LLM APIs may incur costs - monitor usage if billing is a concern. In addition, certain providers might impose **rate limits**, which are not (yet) handled by our implementation as it is vendor specific behaviour.
- **Performance:** LLM responses add latency to a crawl; this extractor is best used for high-value pages or specific use-cases.

## Developer Notes

For local testing, you can use a locally running instance of [Ollama](https://github.com/ollama/ollama), which provides an OpenAI-compatible API interface.

### Setup Instructions

- **Start Ollama** locally (either directly on your system or via Docker).
- **Configure the following properties** in your application:

```yaml
   textextractor.llm.api_key: ""
   textextractor.llm.url: "http://localhost:11434/v1"
   textextractor.llm.model: "your-model-to-test"
```

- Ensure that the specified model (e.g., llama2, mistral, etc.) is already downloaded and ready for use in your local Ollama instance.
