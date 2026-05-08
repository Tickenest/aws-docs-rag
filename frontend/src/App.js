import { useState, useEffect, useCallback } from "react";
import SearchBar from "./components/SearchBar";
import ServiceFilter from "./components/ServiceFilter";
import AnswerPanel from "./components/AnswerPanel";
import SourcesList from "./components/SourcesList";
import LoadingIndicator from "./components/LoadingIndicator";
import "./App.css";

const API_URL    = process.env.REACT_APP_API_URL;
const API_KEY    = process.env.REACT_APP_API_KEY;

const SERVICES = [
  { value: "all",          label: "All Services" },
  { value: "lambda",       label: "Lambda" },
  { value: "s3",           label: "S3" },
  { value: "dynamodb",     label: "DynamoDB" },
  { value: "apigateway",   label: "API Gateway" },
  { value: "bedrock",      label: "Bedrock" },
  { value: "eventbridge",  label: "EventBridge" },
  { value: "iam",          label: "IAM" },
  { value: "ec2",          label: "EC2" },
];

export default function App() {
  const [question, setQuestion]   = useState("");
  const [service, setService]     = useState("all");
  const [loading, setLoading]     = useState(false);
  const [answer, setAnswer]       = useState(null);
  const [sources, setSources]     = useState([]);
  const [error, setError]         = useState(null);
  const [darkMode, setDarkMode]   = useState(() => {
    return localStorage.getItem("darkMode") !== "false";
  });

  useEffect(() => {
    document.body.classList.toggle("dark", darkMode);
    localStorage.setItem("darkMode", darkMode);
  }, [darkMode]);

  const handleQuery = useCallback(async () => {
    if (!question.trim() || loading) return;

    setLoading(true);
    setAnswer(null);
    setSources([]);
    setError(null);

    try {
      const res = await fetch(API_URL, {
        method:  "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key":    API_KEY,
        },
        body: JSON.stringify({ question: question.trim(), service }),
      });

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.error || `HTTP ${res.status}`);
      }

      setAnswer(data.answer);
      setSources(data.sources || []);
    } catch (err) {
      setError(err.message || "Something went wrong. Please try again.");
    } finally {
      setLoading(false);
    }
  }, [question, service, loading]);

  const handleKeyDown = useCallback((e) => {
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
      handleQuery();
    }
  }, [handleQuery]);

  const handleReset = () => {
    setQuestion("");
    setService("all");
    setAnswer(null);
    setSources([]);
    setError(null);
  };

  return (
    <div className="app" onKeyDown={handleKeyDown}>
      <header className="header">
        <div className="header-left">
          <div className="logo">
            <span className="logo-bracket">[</span>
            <span className="logo-text">aws</span>
            <span className="logo-dot">·</span>
            <span className="logo-text2">docs</span>
            <span className="logo-bracket">]</span>
          </div>
          <p className="tagline">Ask anything about AWS documentation</p>
        </div>
        <div className="header-right">
          <button
            className="theme-toggle"
            onClick={() => setDarkMode(d => !d)}
            title={darkMode ? "Switch to light mode" : "Switch to dark mode"}
            aria-label="Toggle dark mode"
          >
            {darkMode ? "☀" : "☾"}
          </button>
        </div>
      </header>

      <main className="main">
        <div className="query-section">
          <ServiceFilter
            services={SERVICES}
            selected={service}
            onChange={setService}
          />
          <SearchBar
            value={question}
            onChange={setQuestion}
            onSubmit={handleQuery}
            loading={loading}
            placeholder="How do I configure a Lambda function URL? (Ctrl+Enter to search)"
          />
        </div>

        {loading && <LoadingIndicator />}

        {error && (
          <div className="error-panel">
            <span className="error-icon">⚠</span>
            <span>{error}</span>
          </div>
        )}

        {answer && !loading && (
          <div className="results">
            <AnswerPanel answer={answer} />
            {sources.length > 0 && (
              <SourcesList sources={sources} />
            )}
            <button className="reset-btn" onClick={handleReset}>
              ← New question
            </button>
          </div>
        )}

        {!answer && !loading && !error && (
          <div className="empty-state">
            <div className="empty-grid">
              {["Lambda", "S3", "DynamoDB", "API Gateway", "Bedrock", "EventBridge", "IAM", "EC2"].map(s => (
                <div key={s} className="empty-service-tag">{s}</div>
              ))}
            </div>
            <p className="empty-hint">
              Search across 38,000+ documentation chunks from 8 AWS services
            </p>
          </div>
        )}
      </main>

      <footer className="footer">
        <span>Powered by Amazon Bedrock · S3 Vectors · Claude Haiku</span>
      </footer>
    </div>
  );
}
