export default function LoadingIndicator() {
  return (
    <div className="loading-indicator">
      <div className="loading-bar-wrap">
        <div className="loading-bar" />
      </div>
      <div className="loading-steps">
        <span className="loading-step">embedding query...</span>
        <span className="loading-step">searching 38,143 chunks...</span>
        <span className="loading-step">generating answer...</span>
      </div>
    </div>
  );
}
