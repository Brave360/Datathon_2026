import { FormEvent, useEffect, useMemo, useState } from "react";
import RankedList from "./components/RankedList";
import ListingsMap from "./components/ListingsMap";

type ListingData = {
  id: string;
  title: string;
  description?: string | null;
  city?: string | null;
  canton?: string | null;
  latitude?: number | null;
  longitude?: number | null;
  image_urls?: string[] | null;
  hero_image_url?: string | null;
  price_chf?: number | null;
  rooms?: number | null;
  living_area_sqm?: number | null;
  features?: string[];
  original_listing_url?: string | null;
};

type RankedListingResult = {
  listing_id: string;
  score: number;
  reason: string;
  listing: ListingData;
};

type ToolOutput = {
  listings?: RankedListingResult[];
  meta?: Record<string, unknown>;
};

type ConversationTurn = {
  role: "user" | "assistant";
  content: string;
};

declare global {
  interface Window {
    openai?: {
      toolOutput?: ToolOutput;
    };
  }
}

type UiToolResultMessage = {
  jsonrpc?: string;
  method?: string;
  params?: {
    structuredContent?: ToolOutput;
  };
};

function readToolOutput(): ToolOutput {
  return window.openai?.toolOutput ?? {};
}

function getApiBaseUrl(): string {
  const configured = import.meta.env.VITE_LISTINGS_API_BASE_URL as string | undefined;
  if (configured && configured.trim()) {
    return configured.replace(/\/$/, "");
  }

  const { protocol, hostname } = window.location;
  return `${protocol}//${hostname}:8000`;
}

function readToolOutputFromMessage(message: unknown): ToolOutput | null {
  if (!message || typeof message !== "object") {
    return null;
  }

  const maybeToolResult = message as UiToolResultMessage;
  if (
    maybeToolResult.jsonrpc !== "2.0" ||
    maybeToolResult.method !== "ui/notifications/tool-result"
  ) {
    return null;
  }

  return maybeToolResult.params?.structuredContent ?? {};
}

export default function App() {
  const [toolOutput, setToolOutput] = useState<ToolOutput>(() => readToolOutput());
  const [query, setQuery] = useState("2.5 to 3 room apartment in Zurich with balcony under 3200 CHF");
  const [limit, setLimit] = useState(12);
  const [isLoading, setIsLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [conversation, setConversation] = useState<ConversationTurn[]>([]);
  const [mode, setMode] = useState<"browser" | "mcp">(() =>
    readToolOutput().listings?.length ? "mcp" : "browser",
  );
  const [selectedId, setSelectedId] = useState<string | null>(null);

  useEffect(() => {
    const onGlobals = (event: Event) => {
      const customEvent = event as CustomEvent<{ globals?: { toolOutput?: ToolOutput } }>;
      const nextOutput = customEvent.detail?.globals?.toolOutput ?? readToolOutput();
      setToolOutput(nextOutput);
      if (nextOutput.listings?.length) {
        setMode("mcp");
        setErrorMessage(null);
      }
    };

    window.addEventListener("openai:set_globals", onGlobals as EventListener);

    const onMessage = (event: MessageEvent) => {
      if (event.source !== window.parent) {
        return;
      }

      const nextToolOutput = readToolOutputFromMessage(event.data);
      if (nextToolOutput) {
        setToolOutput(nextToolOutput);
        if (nextToolOutput.listings?.length) {
          setMode("mcp");
          setErrorMessage(null);
        }
      }
    };

    window.addEventListener("message", onMessage, { passive: true });
    return () => {
      window.removeEventListener("openai:set_globals", onGlobals as EventListener);
      window.removeEventListener("message", onMessage);
    };
  }, []);

  const results = toolOutput.listings ?? [];
  const extractedHardFilters = toolOutput.meta?.extracted_hard_filters;
  const hasToolResults = results.length > 0;
  const shouldShowNoResultsWarning =
    mode === "browser" && !isLoading && !errorMessage && query.trim() === "" && !results.length;

  useEffect(() => {
    if (!results.length) {
      setSelectedId(null);
      return;
    }
    setSelectedId((current) =>
      current && results.some((result) => result.listing_id === current)
        ? current
        : results[0].listing_id,
    );
  }, [results]);

  const selectedListing = useMemo(
    () => results.find((result) => result.listing_id === selectedId) ?? null,
    [results, selectedId],
  );

  async function runSearch(nextQuery: string, nextLimit: number): Promise<void> {
    setIsLoading(true);
    setErrorMessage(null);
    setMode("browser");

    try {
      const response = await fetch(`${getApiBaseUrl()}/listings`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: nextQuery,
          conversation,
          limit: nextLimit,
          offset: 0,
        }),
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Request failed with status ${response.status}`);
      }

      const payload = (await response.json()) as ToolOutput;
      setToolOutput(payload);
      setConversation((current) => [
        ...current,
        { role: "user", content: nextQuery },
        {
          role: "assistant",
          content: buildAssistantSummary(payload),
        },
      ]);
      setQuery("");
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      setToolOutput({ listings: [], meta: {} });
      setErrorMessage(message);
    } finally {
      setIsLoading(false);
    }
  }

  function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    void runSearch(query, limit);
  }

  function resetConversation() {
    setConversation([]);
    setToolOutput({});
    setErrorMessage(null);
    setMode("browser");
    setSelectedId(null);
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-top">
          <section className="search-panel">
            <p className="eyebrow">Swiss Listing Explorer</p>
            <h1>Search the ranking UI directly in your browser</h1>
            <p className="muted hero-copy">
              Natural-language search, extracted hard filters, ranked cards, and the same map view
              as the MCP widget.
            </p>

            <form className="search-form" onSubmit={onSubmit}>
              <label className="search-label" htmlFor="query">
                Search query
              </label>
              <textarea
                id="query"
                className="search-input"
                rows={4}
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Describe the apartment you want..."
              />

              <div className="search-controls">
                <label className="limit-field" htmlFor="limit">
                  <span>Results</span>
                  <input
                    id="limit"
                    type="number"
                    min={1}
                    max={50}
                    value={limit}
                    onChange={(event) => setLimit(Number(event.target.value) || 1)}
                  />
                </label>
                <button className="search-button" type="submit" disabled={isLoading || !query.trim()}>
                  {isLoading ? "Searching..." : "Search listings"}
                </button>
              </div>
            </form>

            <div className="status-row">
              <span className={`mode-pill ${mode}`}>{mode === "mcp" ? "MCP mode" : "Browser mode"}</span>
              <span className="muted">
                {results.length
                  ? `${results.length} result${results.length === 1 ? "" : "s"}`
                  : "No results yet"}
              </span>
            </div>

            {conversation.length ? (
              <div className="conversation-toolbar">
                <button className="reset-button" type="button" onClick={resetConversation}>
                  New conversation
                </button>
              </div>
            ) : null}

            {errorMessage ? <div className="error-banner">{errorMessage}</div> : null}
            {shouldShowNoResultsWarning ? (
              <div className="error-banner">
                No listings matched this search. Try expanding your search area, increasing your
                budget, or relaxing one or two constraints.
              </div>
            ) : null}

            <div className="filters-panel">
              <div className="filters-header">
                <h2>Extracted hard filters</h2>
                <span className="muted">
                  {hasToolResults ? "From latest search" : "Will appear after search"}
                </span>
              </div>
              <pre className="filters-code">
                {JSON.stringify(extractedHardFilters ?? {}, null, 2)}
              </pre>
            </div>
          </section>

          <div className="sidebar-header">
            <p className="eyebrow">Listings</p>
            <h2>Ranked results</h2>
          </div>
        </div>
        <RankedList
          results={results}
          selectedId={selectedId}
          onSelect={setSelectedId}
          emptyTitle={mode === "mcp" ? "No widget data yet." : "No results yet."}
          emptyMessage={
            mode === "mcp"
              ? "Run the search_listings tool to render the map and list."
              : "Run a browser search above to populate the ranked list."
          }
        />
      </aside>
      <main className="map-panel">
        <ListingsMap
          results={results}
          selectedId={selectedId}
          selectedListing={selectedListing}
          onSelect={setSelectedId}
        />
      </main>
    </div>
  );
}

function buildAssistantSummary(payload: ToolOutput): string {
  const extractedHardFilters = payload.meta?.extracted_hard_filters ?? {};
  const resultCount = Array.isArray(payload.listings) ? payload.listings.length : 0;
  return `Previous hard filters: ${JSON.stringify(extractedHardFilters)}. Returned ${resultCount} listings.`;
}
