export default function ServiceFilter({ services, selected, onChange }) {
  return (
    <div className="service-filter">
      <span className="service-filter-label">Scope:</span>
      <div className="service-chips">
        {services.map(s => (
          <button
            key={s.value}
            className={`service-chip${selected === s.value ? " active" : ""}`}
            onClick={() => onChange(s.value)}
          >
            {s.label}
          </button>
        ))}
      </div>
    </div>
  );
}
