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
  meta?: {
    assistant_summary?: string;
    effective_hard_filters?: unknown;
    effective_soft_filters?: unknown;
    extracted_hard_filters?: unknown;
    score_component_weights?: Record<string, number>;
    score_weight_controls?: ScoreWeightControl[];
    [key: string]: unknown;
  };
};

type ConversationTurn = {
  role: "user" | "assistant";
  content: string;
};

type ConversationHistoryResponse = {
  conversation_id: string;
  messages: ConversationTurn[];
};

type ScoreWeightControl = {
  key: string;
  label: string;
  description: string;
  weight: number;
  min_weight: number;
  max_weight: number;
  default_weight: number;
};

type SearchContext = {
  query: string;
  conversation: ConversationTurn[];
  limit: number;
};

function createConversationId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `conversation-${Date.now()}`;
}

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
  const [conversationId, setConversationId] = useState(() => createConversationId());
  const [historyMessages, setHistoryMessages] = useState<ConversationTurn[]>([]);
  const [isHistoryOpen, setIsHistoryOpen] = useState(true);
  const [isHistoryLoading, setIsHistoryLoading] = useState(false);
  const [scoreComponentWeights, setScoreComponentWeights] = useState<Record<string, number>>({});
  const [draftScoreComponentWeights, setDraftScoreComponentWeights] = useState<Record<string, number>>({});
  const [lastSearchContext, setLastSearchContext] = useState<SearchContext | null>(null);
  const [isApplyingPreferenceChanges, setIsApplyingPreferenceChanges] = useState(false);
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
  const extractedHardFilters =
    toolOutput.meta?.effective_hard_filters ?? toolOutput.meta?.extracted_hard_filters ?? {};
  const extractedSoftFilters = toolOutput.meta?.effective_soft_filters ?? {};
  const scoreWeightControls = toolOutput.meta?.score_weight_controls ?? [];
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
  const hasPreferenceWeightChanges = useMemo(
    () =>
      scoreWeightControls.some(
        (control) =>
          Math.abs((draftScoreComponentWeights[control.key] ?? control.default_weight) - (scoreComponentWeights[control.key] ?? control.default_weight)) >
          0.001,
      ),
    [draftScoreComponentWeights, scoreComponentWeights, scoreWeightControls],
  );

  useEffect(() => {
    if (!scoreWeightControls.length) {
      setScoreComponentWeights({});
      setDraftScoreComponentWeights({});
      return;
    }

    setScoreComponentWeights((current) => {
      const next: Record<string, number> = {};
      for (const control of scoreWeightControls) {
        next[control.key] = toolOutput.meta?.score_component_weights?.[control.key] ?? current[control.key] ?? control.weight ?? control.default_weight;
      }
      return next;
    });
    setDraftScoreComponentWeights((current) => {
      const next: Record<string, number> = {};
      for (const control of scoreWeightControls) {
        next[control.key] = current[control.key] ?? toolOutput.meta?.score_component_weights?.[control.key] ?? control.weight ?? control.default_weight;
      }
      return next;
    });
  }, [scoreWeightControls, toolOutput.meta?.score_component_weights]);

  async function fetchHistory(nextConversationId: string): Promise<void> {
    setIsHistoryLoading(true);
    try {
      const response = await fetch(`${getApiBaseUrl()}/listings/history/${nextConversationId}`);
      if (!response.ok) {
        if (response.status === 404) {
          setHistoryMessages([]);
          return;
        }
        const text = await response.text();
        throw new Error(text || `Request failed with status ${response.status}`);
      }

      const payload = (await response.json()) as ConversationHistoryResponse;
      setHistoryMessages(payload.messages ?? []);
    } finally {
      setIsHistoryLoading(false);
    }
  }

  useEffect(() => {
    if (!isHistoryOpen) {
      return;
    }

    void fetchHistory(conversationId).catch((error: unknown) => {
      const message = error instanceof Error ? error.message : "Unknown error";
      setErrorMessage(message);
    });
  }, [conversationId, isHistoryOpen]);

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
          conversation_id: conversationId,
          conversation,
          soft_preference_weights: scoreComponentWeights,
          limit: nextLimit,
          offset: 0,
        }),
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Request failed with status ${response.status}`);
      }

      const payload = (await response.json()) as ToolOutput;
      const assistantSummary = payload.meta?.assistant_summary ?? buildAssistantSummary(payload);
      setToolOutput(payload);
      setLastSearchContext({
        query: nextQuery,
        conversation,
        limit: nextLimit,
      });
      const nextTurns = [
        { role: "user" as const, content: nextQuery },
        { role: "assistant" as const, content: assistantSummary },
      ];
      setConversation((current) => [...current, ...nextTurns]);
      if (isHistoryOpen) {
        await fetchHistory(conversationId);
      }
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
    setConversationId(createConversationId());
    setHistoryMessages([]);
    setIsHistoryOpen(true);
    setScoreComponentWeights({});
    setDraftScoreComponentWeights({});
    setLastSearchContext(null);
    setToolOutput({});
    setErrorMessage(null);
    setMode("browser");
    setSelectedId(null);
  }

  async function applyPreferenceChanges(): Promise<void> {
    if (!lastSearchContext || !scoreWeightControls.length) {
      return;
    }

    setIsApplyingPreferenceChanges(true);
    setErrorMessage(null);
    try {
      const response = await fetch(`${getApiBaseUrl()}/listings/rerank`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: lastSearchContext.query,
          conversation_id: conversationId,
          conversation: lastSearchContext.conversation,
          soft_preference_weights: draftScoreComponentWeights,
          limit: lastSearchContext.limit,
          offset: 0,
        }),
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Request failed with status ${response.status}`);
      }

      const payload = (await response.json()) as ToolOutput;
      setToolOutput(payload);
      setScoreComponentWeights({ ...draftScoreComponentWeights });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      setErrorMessage(message);
    } finally {
      setIsApplyingPreferenceChanges(false);
    }
  }

  async function toggleHistory(): Promise<void> {
    if (isHistoryOpen) {
      setIsHistoryOpen(false);
      return;
    }

    try {
      await fetchHistory(conversationId);
      setIsHistoryOpen(true);
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      setErrorMessage(message);
    }
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

            <div className="conversation-toolbar">
              <button className="reset-button" type="button" onClick={resetConversation}>
                New conversation
              </button>
              <button className="reset-button" type="button" onClick={() => void toggleHistory()}>
                {isHistoryLoading
                  ? "Loading history..."
                  : isHistoryOpen
                    ? "Hide message history"
                    : "Show message history"}
              </button>
            </div>

            {isHistoryOpen ? (
              <div className="history-panel">
                <div className="history-header">
                  <h2>Message history</h2>
                  <span className="muted">
                    {historyMessages.length
                      ? `${historyMessages.length} message${historyMessages.length === 1 ? "" : "s"}`
                      : "No messages yet"}
                  </span>
                </div>
                <div className="history-list">
                  {historyMessages.length ? (
                    historyMessages.map((message, index) => (
                      <article key={`${message.role}-${index}`} className={`history-message ${message.role}`}>
                        <div className="history-role">{message.role}</div>
                        <div className="history-content">{message.content}</div>
                      </article>
                    ))
                  ) : (
                    <p className="muted history-empty">
                      This conversation has not sent any stored messages yet.
                    </p>
                  )}
                </div>
              </div>
            ) : null}

            {scoreWeightControls.length ? (
              <div className="preference-panel">
                <div className="preference-header">
                  <h2>Tune reranking balance</h2>
                  <span className="muted">Adjust how much each kind of soft preference should influence the ranking</span>
                </div>
                <div className="preference-controls">
                  {scoreWeightControls.map((control) => {
                    const value = draftScoreComponentWeights[control.key] ?? control.default_weight;
                    return (
                      <label key={control.key} className="preference-control">
                        <div className="preference-copy">
                          <div className="preference-title-row">
                            <span className="preference-title">{control.label}</span>
                            <span className="preference-strength">{describeWeight(value)}</span>
                          </div>
                          <div className="muted preference-description">{control.description}</div>
                        </div>
                        <input
                          className="preference-slider"
                          type="range"
                          min={control.min_weight}
                          max={control.max_weight}
                          step={0.1}
                          value={value}
                          onChange={(event) =>
                            setDraftScoreComponentWeights((current) => ({
                              ...current,
                              [control.key]: Number(event.target.value),
                            }))
                          }
                        />
                      </label>
                    );
                  })}
                </div>
                <div className="preference-actions">
                  <button
                    className="search-button preference-apply-button"
                    type="button"
                    disabled={isApplyingPreferenceChanges || !hasPreferenceWeightChanges}
                    onClick={() => void applyPreferenceChanges()}
                  >
                    {isApplyingPreferenceChanges ? "Updating ranking..." : "Apply preference changes"}
                  </button>
                  {hasPreferenceWeightChanges ? (
                    <button
                      className="reset-button"
                      type="button"
                      onClick={() => setDraftScoreComponentWeights({ ...scoreComponentWeights })}
                    >
                      Reset unsaved changes
                    </button>
                  ) : null}
                </div>
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
                <h2>Extracted filters</h2>
                <span className="muted">
                  {hasToolResults ? "From latest search" : "Will appear after search"}
                </span>
              </div>
              <div className="filters-sections">
                <section className="filter-section">
                  <div className="filter-section-header">Hard filters</div>
                  <pre className="filters-code">{JSON.stringify(extractedHardFilters, null, 2)}</pre>
                </section>
                <section className="filter-section">
                  <div className="filter-section-header">Soft filters</div>
                  <pre className="filters-code">{JSON.stringify(extractedSoftFilters, null, 2)}</pre>
                </section>
              </div>
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
  const extractedHardFilters = payload.meta?.effective_hard_filters ?? payload.meta?.extracted_hard_filters ?? {};
  const extractedSoftFilters = payload.meta?.effective_soft_filters ?? {};
  const resultCount = Array.isArray(payload.listings) ? payload.listings.length : 0;
  return `Previous hard filters: ${JSON.stringify(extractedHardFilters)}. Previous soft filters: ${JSON.stringify(extractedSoftFilters)}. Returned ${resultCount} listings.`;
}

function describeWeight(value: number): string {
  if (value <= 0.2) {
    return "Off";
  }
  if (value < 0.8) {
    return "Light";
  }
  if (value < 1.3) {
    return "Balanced";
  }
  if (value < 1.7) {
    return "Strong";
  }
  return "Very strong";
}
