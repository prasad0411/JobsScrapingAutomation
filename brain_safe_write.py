"""
FIX 2 — Stop the two brains from erasing each other.

THE PROBLEM (plain terms):
Two different pieces of code both save to the same file, brain.json.
Each one does it the naive way: "load my copy, change my copy, write my
WHOLE copy back over the file." If they ever run close together, the second
one to save overwrites everything the first one just learned — silently.
Imagine two people editing the same document by each downloading it, making
their edit, and re-uploading the entire file. Whoever uploads last wins;
the other person's edit vanishes with no error.

THE FIX:
A single helper both of them call. It does two safe things:
  1. Takes a file lock, so only one writer touches brain.json at a time.
     The other waits its turn instead of overwriting.
  2. Re-reads the file FRESH inside the lock, applies only the change,
     then writes. So it merges with whatever the other writer just saved
     instead of clobbering it.

This changes nothing about WHAT gets learned. It only makes sure two
simultaneous learners don't delete each other's work.
"""
import json
import os
import fcntl  # file locking on macOS/Linux
import tempfile

BRAIN_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".local", "brain.json"
)


def update_brain(mutator, brain_file: str = BRAIN_FILE):
    """
    Safely apply a change to brain.json.

    `mutator` is a function you write that takes the current brain dict and
    changes it in place (or returns a new one). Example:

        def add_slug(data):
            data.setdefault("learned_slugs", {})["leonardodrs"] = "Leonardo DRS"
        update_brain(add_slug)

    Guarantees: only one process writes at a time, and the change is applied
    on top of the LATEST version of the file, not a stale in-memory copy.
    """
    os.makedirs(os.path.dirname(brain_file), exist_ok=True)
    # Open (or create) a lock file next to the brain.
    lock_path = brain_file + ".lock"
    with open(lock_path, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)   # wait here until it's our turn
        try:
            # Re-read FRESH so we merge with anyone who just wrote.
            data = {}
            if os.path.exists(brain_file):
                try:
                    with open(brain_file, encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    data = {}  # corrupt? start clean rather than crash

            result = mutator(data)
            if result is not None:      # allow mutator to return a new dict
                data = result

            # Write to a temp file then rename — a crash mid-write can never
            # leave brain.json half-written (rename is atomic on the same disk).
            dir_ = os.path.dirname(brain_file) or "."
            fd, tmp = tempfile.mkstemp(dir=dir_, suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                os.replace(tmp, brain_file)   # atomic swap
            except Exception:
                if os.path.exists(tmp):
                    os.remove(tmp)
                raise
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


# ---- How each caller changes ----
#
# In quality_gate.py, the Brain class methods like add_slug_fix become:
#
#     from brain_safe_write import update_brain
#     def add_slug_fix(self, slug, correct):
#         update_brain(lambda d: d.setdefault("learned_slugs", {})
#                                  .__setitem__(slug.lower(), correct))
#
# In pipeline_brain.py, PipelineBrain.save() becomes a merge instead of a
# blind full-file dump:
#
#     def save(self):
#         if not self._dirty: return
#         def merge(disk):
#             disk.update(self.data)   # our changes win for OUR keys only
#             return disk
#         update_brain(merge)
#         self._dirty = False
#
# Note: the merge above still lets each writer own its own keys. If both ever
# write the SAME key you'd want smarter merging, but today they write
# different keys (learned_slugs vs domains/companies), so update() is safe.
