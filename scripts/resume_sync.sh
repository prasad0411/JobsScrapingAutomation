#!/bin/bash
# resume_sync.sh — auto-sync latest resumes from Downloads to .local/
# Run by launchd on wake, before aggregator/outreach

DOWNLOADS="/Users/prasadkanade/Downloads"
LOCAL="/Users/prasadkanade/Documents/Prasad Kanade/Job Hunt Tracker/.local"
LOG="$LOCAL/resume_sync.log"

echo "=== Resume sync at $(date) ===" >> "$LOG"

sync_resume() {
    local filename="$1"
    local label="$2"
    local src="$DOWNLOADS/$filename"
    local dst="$LOCAL/$filename"

    if [ -f "$src" ]; then
        if [ ! -f "$dst" ] || [ "$src" -nt "$dst" ]; then
            cp "$src" "$dst"
            echo "  ✓ Updated $label resume from Downloads" >> "$LOG"
        else
            echo "  ℹ $label resume already up to date" >> "$LOG"
        fi
    else
        echo "  ⚠ $label resume not found in Downloads: $filename" >> "$LOG"
    fi
}

sync_resume "Prasad Kanade SWE Resume.pdf" "SWE"
sync_resume "Prasad Kanade ML Resume.pdf" "ML"
sync_resume "Prasad Kanade Data Resume.pdf" "Data"

echo "=== Resume sync done ===" >> "$LOG"
