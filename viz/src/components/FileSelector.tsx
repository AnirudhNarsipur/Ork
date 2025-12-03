import { useEffect, useState } from "react";

interface FileSelectorProps {
  onFileSelect: (fileName: string) => void;
  currentFile: string | null;
  isWatching: boolean;
}

export function FileSelector({
  onFileSelect,
  currentFile,
  isWatching,
}: FileSelectorProps) {
  const [files, setFiles] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchFiles() {
      try {
        const response = await fetch("/api/event-logs");
        if (!response.ok) {
          throw new Error("Failed to fetch event logs");
        }
        const data = await response.json();
        setFiles(data.files);
        setError(null);
      } catch (err) {
        setError(`${err}`);
      } finally {
        setLoading(false);
      }
    }

    fetchFiles();
  }, []);

  const handleChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const selected = e.target.value;
    if (selected) {
      onFileSelect(selected);
    }
  };

  if (loading) {
    return <div className="file-selector">Loading event logs...</div>;
  }

  if (error) {
    return <div className="file-selector"><span className="file-error">{error}</span></div>;
  }

  return (
    <div className="file-selector">
      <select
        className="file-dropdown"
        value={currentFile || ""}
        onChange={handleChange}
      >
        <option value="">Select an event log...</option>
        {files.map((file) => (
          <option key={file} value={file}>
            {file}
          </option>
        ))}
      </select>
      {currentFile && isWatching && (
        <span className="watching-indicator">watching</span>
      )}
    </div>
  );
}
