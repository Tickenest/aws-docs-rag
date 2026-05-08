export default function SearchBar({ value, onChange, onSubmit, loading, placeholder }) {
  return (
    <div className="search-bar">
      <div className="search-input-wrap">
        <textarea
          className="search-input"
          value={value}
          onChange={e => onChange(e.target.value)}
          placeholder={placeholder}
          rows={2}
          disabled={loading}
        />
      </div>
      <button
        className="search-btn"
        onClick={onSubmit}
        disabled={loading || !value.trim()}
      >
        {loading ? "..." : "Search"}
      </button>
    </div>
  );
}
