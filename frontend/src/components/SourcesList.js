export default function SourcesList({ sources }) {
  return (
    <div className="sources-panel">
      <div className="sources-header">
        <span className="sources-header-label">Sources</span>
        <span className="sources-count">{sources.length}</span>
      </div>
      <ul className="sources-list">
        {sources.map((source, i) => (
          <li key={source.source_url} className="source-item">
            <span className="source-index">{i + 1}.</span>
            <div className="source-content">
              <a
                className="source-link"
                href={source.source_url}
                target="_blank"
                rel="noopener noreferrer"
                title={source.page_title}
              >
                {source.page_title}
              </a>
              <div className="source-meta">
                <span className="source-service">{source.service}</span>
                <span className="source-heading">{source.heading}</span>
                <span className="source-distance">
                  {(source.distance * 100).toFixed(1)}% match
                </span>
              </div>
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}
