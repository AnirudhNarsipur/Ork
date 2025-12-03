import { useState, useCallback, useEffect, useRef } from "react";
import { FileSelector } from "./components/FileSelector";
import { TaskGraph } from "./components/TaskGraph";
import { NavigationControls } from "./components/NavigationControls";
import { PromisesPanel } from "./components/PromisesPanel";
import { ScratchPanel } from "./components/ScratchPanel";
import type { Event, WorkflowState } from "./types";
import {
  parseEvents,
  filterNonMessageEvents,
  buildStateAtIndex,
  createEmptyState,
} from "./workflowState";
import "./App.css";

const POLL_INTERVAL = 500;

function App() {
  const [nonMessageEvents, setNonMessageEvents] = useState<Event[]>([]);
  const [currentIndex, setCurrentIndex] = useState(-1);
  const [currentFile, setCurrentFile] = useState<string | null>(null);
  const [workflowState, setWorkflowState] = useState<WorkflowState>(createEmptyState());
  const [autoFollow, setAutoFollow] = useState(true);
  const [isWatching, setIsWatching] = useState(false);

  // Use refs for values accessed in polling callback to avoid stale closures
  const currentIndexRef = useRef(currentIndex);
  const autoFollowRef = useRef(autoFollow);
  const prevEventCountRef = useRef(0);
  const pollIntervalRef = useRef<number | null>(null);
  const lastContentRef = useRef<string | null>(null);

  // Keep refs in sync with state
  useEffect(() => {
    currentIndexRef.current = currentIndex;
  }, [currentIndex]);

  useEffect(() => {
    autoFollowRef.current = autoFollow;
  }, [autoFollow]);

  const processContent = useCallback((content: string) => {
    // Skip if content hasn't changed
    if (content === lastContentRef.current) {
      return;
    }
    lastContentRef.current = content;

    const events = parseEvents(content);
    const filtered = filterNonMessageEvents(events);

    const prevCount = prevEventCountRef.current;
    const currIndex = currentIndexRef.current;
    const shouldAutoFollow = autoFollowRef.current;

    setNonMessageEvents(filtered);

    if (filtered.length > 0) {
      // Only update index/state if:
      // 1. This is initial load (currIndex === -1)
      // 2. New events arrived AND we're auto-following
      const isInitialLoad = currIndex === -1;
      const hasNewEvents = filtered.length > prevCount;
      const wasAtEnd = prevCount === 0 || currIndex >= prevCount - 1;

      if (isInitialLoad) {
        // Initial load - jump to end
        const lastIndex = filtered.length - 1;
        setCurrentIndex(lastIndex);
        setWorkflowState(buildStateAtIndex(filtered, lastIndex));
      } else if (hasNewEvents && shouldAutoFollow && wasAtEnd) {
        // New events and auto-following - jump to new end
        const lastIndex = filtered.length - 1;
        setCurrentIndex(lastIndex);
        setWorkflowState(buildStateAtIndex(filtered, lastIndex));
      }
      // Otherwise: don't touch currentIndex or workflowState

      prevEventCountRef.current = filtered.length;
    }
  }, []);

  const fetchFileContent = useCallback(async (fileName: string) => {
    try {
      const response = await fetch(`/api/event-log/${encodeURIComponent(fileName)}`);
      if (!response.ok) {
        throw new Error(`Failed to fetch ${fileName}`);
      }
      const content = await response.text();
      processContent(content);
    } catch (err) {
      console.error("Error fetching file:", err);
    }
  }, [processContent]);

  const handleFileSelect = useCallback((fileName: string) => {
    // Stop any existing polling
    if (pollIntervalRef.current) {
      clearInterval(pollIntervalRef.current);
    }

    // Reset state for new file
    setCurrentFile(fileName);
    setAutoFollow(true);
    autoFollowRef.current = true;
    prevEventCountRef.current = 0;
    lastContentRef.current = null;
    setCurrentIndex(-1);
    currentIndexRef.current = -1;

    // Initial fetch
    fetchFileContent(fileName);

    // Start polling for updates
    pollIntervalRef.current = window.setInterval(() => {
      fetchFileContent(fileName);
    }, POLL_INTERVAL);

    setIsWatching(true);
  }, [fetchFileContent]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
      }
    };
  }, []);

  const handlePrevious = useCallback(() => {
    if (currentIndex > 0) {
      setAutoFollow(false);
      const newIndex = currentIndex - 1;
      setCurrentIndex(newIndex);
      setWorkflowState(buildStateAtIndex(nonMessageEvents, newIndex));
    }
  }, [currentIndex, nonMessageEvents]);

  const handleNext = useCallback(() => {
    if (currentIndex < nonMessageEvents.length - 1) {
      const newIndex = currentIndex + 1;
      setCurrentIndex(newIndex);
      setWorkflowState(buildStateAtIndex(nonMessageEvents, newIndex));
      if (newIndex === nonMessageEvents.length - 1) {
        setAutoFollow(true);
      }
    }
  }, [currentIndex, nonMessageEvents]);

  const handleJumpToStart = useCallback(() => {
    if (nonMessageEvents.length > 0) {
      setAutoFollow(false);
      setCurrentIndex(0);
      setWorkflowState(buildStateAtIndex(nonMessageEvents, 0));
    }
  }, [nonMessageEvents]);

  const handleJumpToEnd = useCallback(() => {
    if (nonMessageEvents.length > 0) {
      setAutoFollow(true);
      const lastIndex = nonMessageEvents.length - 1;
      setCurrentIndex(lastIndex);
      setWorkflowState(buildStateAtIndex(nonMessageEvents, lastIndex));
    }
  }, [nonMessageEvents]);

  // Keyboard navigation
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "ArrowLeft") {
        handlePrevious();
      } else if (e.key === "ArrowRight") {
        handleNext();
      } else if (e.key === "Home") {
        handleJumpToStart();
      } else if (e.key === "End") {
        handleJumpToEnd();
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [handlePrevious, handleNext, handleJumpToStart, handleJumpToEnd]);

  const currentEvent = currentIndex >= 0 ? nonMessageEvents[currentIndex] : null;

  return (
    <div className="app">
      <header className="app-header">
        <h1>Workflow Visualizer</h1>
        <FileSelector
          onFileSelect={handleFileSelect}
          currentFile={currentFile}
          isWatching={isWatching}
        />
      </header>

      <div className="app-body">
        <aside className="left-panel">
          <ScratchPanel nodes={Array.from(workflowState.nodes.values())} />
        </aside>

        <main className="main-panel">
          {workflowState.shutdown && (
            <div className={`shutdown-banner ${workflowState.shutdown.successful ? "success" : "failure"}`}>
              Workflow {workflowState.shutdown.successful ? "completed successfully" : "failed"}
            </div>
          )}
          <TaskGraph state={workflowState} />
          <NavigationControls
            currentIndex={currentIndex}
            totalEvents={nonMessageEvents.length}
            onPrevious={handlePrevious}
            onNext={handleNext}
            onJumpToStart={handleJumpToStart}
            onJumpToEnd={handleJumpToEnd}
          />
          {currentEvent && (
            <div className="current-event-info">
              <span className="event-type">{currentEvent.event.event_type}</span>
              <span className="event-time">
                {new Date(currentEvent.timestamp * 1000).toLocaleTimeString()}
              </span>
              {isWatching && (
                <span className={`auto-follow ${autoFollow ? "active" : ""}`}>
                  {autoFollow ? "Auto-following" : "Paused"}
                </span>
              )}
            </div>
          )}
        </main>

        <aside className="right-panel">
          <PromisesPanel promises={workflowState.promises} />
        </aside>
      </div>
    </div>
  );
}

export default App;
