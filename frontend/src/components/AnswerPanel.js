import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

export default function AnswerPanel({ answer }) {
  return (
    <div className="answer-panel">
      <div className="answer-header">
        <div className="answer-header-dot" />
        <span className="answer-header-label">Answer</span>
      </div>
      <div className="answer-body">
        <ReactMarkdown remarkPlugins={[remarkGfm]}>{answer}</ReactMarkdown>
      </div>
    </div>
  );
}
