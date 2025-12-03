interface NavigationControlsProps {
  currentIndex: number;
  totalEvents: number;
  onPrevious: () => void;
  onNext: () => void;
  onJumpToStart: () => void;
  onJumpToEnd: () => void;
}

export function NavigationControls({
  currentIndex,
  totalEvents,
  onPrevious,
  onNext,
  onJumpToStart,
  onJumpToEnd,
}: NavigationControlsProps) {
  const isAtStart = currentIndex <= 0;
  const isAtEnd = currentIndex >= totalEvents - 1;

  return (
    <div className="navigation-controls">
      <button onClick={onJumpToStart} disabled={isAtStart} title="Jump to start">
        &#x23EE;
      </button>
      <button onClick={onPrevious} disabled={isAtStart} title="Previous event">
        &#x25C0;
      </button>
      <span className="event-counter">
        {totalEvents > 0 ? `${currentIndex + 1} / ${totalEvents}` : "0 / 0"}
      </span>
      <button onClick={onNext} disabled={isAtEnd} title="Next event">
        &#x25B6;
      </button>
      <button onClick={onJumpToEnd} disabled={isAtEnd} title="Jump to end">
        &#x23ED;
      </button>
    </div>
  );
}
