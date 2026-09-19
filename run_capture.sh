#!/bin/bash
# run_capture.sh -- the only thing launchd runs. BUILD STEP 6.
#
# Everything in this file exists because a launchd agent is not a terminal.
# It gets no .zshrc, no .bash_profile, no PATH worth the name, no working
# directory you did not ask for, and no controlling terminal. A script that
# works when you type it and does nothing at all under launchd is the single
# most common way a scheduled job fails, and it fails SILENTLY: launchd runs
# it, the shell cannot find `python`, the script exits 127, and logs/launchd.log
# gains one line nobody reads for a week. So: every binary below is named by
# absolute path, every file is named by absolute path, and the first and last
# thing this script does is write a line to stdout -- which the plist points at
# logs/launchd.log -- so that EVERY fire leaves a trace even when the python
# never started.
#
# TWO LOG FILES, and this script writes to neither directly. Its stdout and
# stderr go to logs/launchd.log because that is where the plist sends them;
# capture_leaderboard.py's own logger writes logs/capture.log. Sending both to
# one file put every python line in it twice. See the plist's StandardOutPath
# comment.
#
# The directory name contains a space ("Trading with Claude Code"). Every
# expansion in this file is quoted, every time. An unquoted "$FOMO_DIR" splits
# into "/Users/reinestarr/Trading", "with", "Claude"... and the error that
# produces points nowhere near the real cause.
#
# THE TIMEOUT IS 1800 SECONDS, and that number is decided, not derived here.
# Today's measured run was 331.6s of wall clock for 22 captures, about 15s
# each. 1800s is roughly 5.4x that, and the margin is there for a cold Chrome
# launch and for boards that are merely slow to render -- so the watchdog
# never kills a run that was about to land its boards, on a day that cannot be
# re-collected. At the same time 1800s is far below the 24h gap to the next
# fire, so a wedged browser cannot sit on .captures.lock and .capture_run.lock
# until tomorrow's run and take that day down with it too. The failure this
# bounds is not slowness; it is a hang.
#
# THERE ARE NO RETRIES IN THAT MARGIN. An earlier version of this comment said
# the budget also absorbed "the script's own two-attempt retries"; there are
# none, and capture_leaderboard.py says so in as many words next to
# INTER_CAPTURE_PAUSE_S: "There is no second attempt anywhere in this file."
# PLAN.md puts backoff and retry queues out of scope. The only pause in a run
# is a flat 2.5s of politeness between captures, which is already inside the
# 331.6s measured above.
#
# macOS ships no timeout(1) and no gtimeout (checked with `which`: both
# absent), so the watchdog is implemented below.

set -uo pipefail
# NOT `set -e`. This script's job is to OBSERVE the python's exit code and
# report it; -e would make the script die at the moment the capture fails,
# before it could write the line saying so.

FOMO_DIR="/Users/reinestarr/Trading with Claude Code/fomo"
PYTHON="$FOMO_DIR/.venv/bin/python"
CAPTURE_SCRIPT="$FOMO_DIR/capture_leaderboard.py"
LOG_DIR="$FOMO_DIR/logs"

TIMEOUT_SECONDS=1800
# How long a doomed process tree gets to shut down politely after SIGTERM
# before it is SIGKILLed. Chrome writes profile state on the way out; 10s is
# enough for that and short enough that the whole teardown fits inside the
# plist's ExitTimeOut.
TERM_GRACE_SECONDS=10

# Absolute paths for every external binary. See the header.
CAFFEINATE="/usr/bin/caffeinate"
PS="/bin/ps"
AWK="/usr/bin/awk"
DATE="/bin/date"
SLEEP="/bin/sleep"
MKDIR="/bin/mkdir"
KILL="/bin/kill"

# The conventional exit code of timeout(1), reused here so a reader who knows
# that tool reads this one correctly, and so 124 never collides with the
# python's own 0/1.
EXIT_TIMED_OUT=124
# Nothing was runnable -- a missing venv or a missing script. 127 is the
# shell's own "command not found", which is what this is.
EXIT_NOT_RUNNABLE=127
EXIT_SIGNALLED=143
# The export below did not take. Its own code because it is its own fault:
# not 0/1 (the python's), not 124 (the watchdog), not 126/127 (the shell's
# "cannot execute" / "not found"), not 143 (a signal). timeout(1) uses 125 for
# "the wrapper itself failed", which is exactly what this is.
#
# ONE HAZARD, IF ANYTHING EVER PARSES THESE: 125 sits directly adjacent to 124,
# the watchdog timeout above, and the two mean opposite things -- 124 is "the
# capture ran too long and was killed mid-board", 125 is "the capture never
# started because this wrapper could not arm it". An off-by-one in a reader, or
# a glance at the wrong digit in a log, inverts the diagnosis. Nothing reads
# them today; the banner line names the reason in words for exactly that
# reason, and anything that starts matching on the number should match on the
# banner text too.
EXIT_NOT_ARMED=125

# Set before any trap can fire: with `set -u`, a trap that reads an unset
# variable kills the shell with a confusing error instead of doing its job.
runner_pid=0
watchdog_pid=0


say() {
    # Stamped in UTC to match capture_leaderboard.py's own log lines, which
    # are UTC because capture_date_utc is. The two interleave in one file.
    #
    # printf and echo below are bash BUILTINS, which is why they are the only
    # commands in this file with no absolute path: there is no PATH lookup to
    # get wrong. Everything that forks a real binary is named absolutely.
    printf '%sZ wrapper %s\n' "$("$DATE" -u '+%Y-%m-%d %H:%M:%S')" "$*"
}


descendants_of() {
    # Every process descended from the pids in $1, those pids included.
    #
    # WHY A PARENT-CHILD WALK AND NOT `kill -- -PGID`. Two reasons, and the
    # second one is the load-bearing one:
    #
    #   1. The plist sets AbandonProcessGroup, so launchd will not reap
    #      anything for us. Whatever this script leaves running stays running.
    #   2. Playwright's node driver spawns Chrome with `detached: true` (read
    #      in the installed driver bundle, lib/coreBundle.js), which means
    #      Chrome calls setsid and becomes the leader of its OWN process
    #      group and session. Signalling our process group would therefore
    #      kill bash, caffeinate, python and node -- and leave a headed Chrome
    #      on screen forever, holding the profile and, worse, looking to the
    #      next run like a browser that is still in use.
    #
    # The PPID link survives setsid, so the parent-child chain in `ps` is the
    # one thread that still connects us to that Chrome. This walks it.
    local roots="$1"
    local table
    table="$("$PS" -Ao pid=,ppid=)"
    local generation="$roots"
    local all="$roots"
    local depth=0
    # 20 generations is far more than bash -> caffeinate -> python -> node ->
    # chrome -> renderer ever needs. The cap is here so a malformed ps table
    # can never turn a teardown into an infinite loop.
    while [ -n "$generation" ] && [ "$depth" -lt 20 ]; do
        local children=""
        local pid
        for pid in $generation; do
            children="$children $(printf '%s\n' "$table" \
                | "$AWK" -v parent="$pid" '$2 == parent { print $1 }')"
        done
        # Unquoted on purpose: word splitting collapses the whitespace, so a
        # generation with no children becomes the empty string and ends the
        # loop. Quoted, " " is non-empty and this never terminates.
        generation="$(echo $children)"
        all="$all $generation"
        depth=$((depth + 1))
    done
    echo $all
}


kill_tree() {
    # Take down the whole capture: caffeinate, python, the node driver, Chrome
    # and Chrome's renderers.
    local root_pid="$1"
    local doomed
    doomed="$(descendants_of "$root_pid")"
    say "killing process tree rooted at $root_pid: $doomed"
    "$KILL" -TERM $doomed 2>/dev/null
    "$SLEEP" "$TERM_GRACE_SECONDS"
    # Re-walked from the ORIGINAL list, not from the root alone. By now the
    # root is usually dead, so a fresh walk from it would find nothing and
    # Chrome -- reparented to launchd the moment its parent died -- would
    # survive the SIGKILL pass. Walking from every pid we already named keeps
    # Chrome in the list whatever happened to node, and picks up anything
    # spawned during the grace period as well.
    doomed="$(descendants_of "$doomed")"
    "$KILL" -KILL $doomed 2>/dev/null
    return 0
}


stop_watchdog() {
    # Disarm a watchdog that is still counting down, and take its `sleep` with
    # it. Killing the subshell alone is not enough: the sleep is the
    # subshell's CHILD, and killing a parent does not kill its children, so
    # every run would strand a `sleep 1800` for up to half an hour. That
    # stray sleep is harmless in itself -- the subshell that was going to do
    # the killing is gone, so nothing can fire -- but a stray process from a
    # finished capture is exactly the kind of thing that sends a person
    # hunting a bug that is not there.
    #
    # No grace period and no SIGKILL pass: everything here is a sleeping shell.
    if [ "$watchdog_pid" -gt 0 ]; then
        "$KILL" -TERM $(descendants_of "$watchdog_pid") 2>/dev/null
        wait "$watchdog_pid" 2>/dev/null
        watchdog_pid=0
    fi
}


on_signal() {
    # `launchctl bootout` / `launchctl kill TERM` signals THIS script only --
    # AbandonProcessGroup means launchd will not touch the browser. Without
    # this trap, stopping the job would leave a visible Chrome window running
    # the rest of the capture with nothing watching it. The plist's ExitTimeOut
    # is sized to let this finish.
    say "received a termination signal; tearing the capture down"
    # The watchdog goes FIRST. It is a child of this shell, so if this shell
    # exits while it is still asleep it is orphaned and still armed: half an
    # hour later it wakes up and kills a process tree rooted at a pid that has
    # long since been reaped and, by then, quite possibly reused by something
    # that has nothing to do with this capture.
    stop_watchdog
    if [ "$runner_pid" -gt 0 ]; then
        kill_tree "$runner_pid"
    fi
    say "==== fomo capture: terminated by signal ===="
    exit "$EXIT_SIGNALLED"
}
trap on_signal TERM INT


say "==== fomo capture: started $("$DATE" -u '+%Y-%m-%dT%H:%M:%SZ') (local $("$DATE" '+%Y-%m-%d %H:%M:%S %Z')) ===="

# This does NOT rescue the launchd case, and nothing in this script could:
# launchd opens StandardOutPath BEFORE it runs ProgramArguments, so if logs/ is
# missing the job fails to spawn and this line is never reached. What fixes
# that case is logs/.gitkeep -- the directory is tracked, so a fresh clone has
# it -- and the `logs/*` + `!logs/.gitkeep` pair in .gitignore is what keeps
# the directory tracked while the log files stay out of the repo.
#
# This line covers what .gitkeep cannot: a logs/ deleted by hand after the
# clone, with someone running this wrapper directly rather than through
# launchd.
"$MKDIR" -p "$LOG_DIR"

if [ ! -x "$PYTHON" ]; then
    say "FATAL: no python at $PYTHON -- the venv is missing or not executable"
    say "==== fomo capture: finished exit=$EXIT_NOT_RUNNABLE elapsed=0s ===="
    exit "$EXIT_NOT_RUNNABLE"
fi
if [ ! -f "$CAPTURE_SCRIPT" ]; then
    say "FATAL: no capture script at $CAPTURE_SCRIPT"
    say "==== fomo capture: finished exit=$EXIT_NOT_RUNNABLE elapsed=0s ===="
    exit "$EXIT_NOT_RUNNABLE"
fi

# The plist sets WorkingDirectory too. Doing it here as well means the wrapper
# behaves identically when it is run by hand, and the `|| exit` means a
# vanished directory is loud rather than a run against whatever launchd's cwd
# happened to be.
cd "$FOMO_DIR" || {
    say "FATAL: cannot cd to $FOMO_DIR"
    exit "$EXIT_NOT_RUNNABLE"
}

started_epoch="$("$DATE" +%s)"

# THE LIVE-CAPTURE INTERLOCK IS ARMED HERE, AND NOWHERE ELSE IN THE SYSTEM.
#
# capture_leaderboard.py refuses to open a browser or fetch a page unless
# FOMO_LIVE_CAPTURE is exactly the string "1". That is deliberate: `capture` is
# its default verb, so a test that invokes it with no arguments falls straight
# through into a real run against solanatracker.io -- which is precisely what
# happened on 2026-09-18, for about two minutes, before it was killed.
#
# WHY THIS LINE AND NOT THE PLIST'S EnvironmentVariables. This wrapper is the
# sanctioned entry point and the one launchd invokes, so putting the export
# here means there is exactly ONE path to the live site and it is auditable in
# one place: grep the repository for FOMO_LIVE_CAPTURE and this is the only
# thing that sets it. Split across the plist as well and there would be two
# doors, one of them invisible to anyone reading the script.
#
# A disarmed run is not silent and is not a clean exit: the python logs which
# mode is in force at the start of EVERY run and exits nonzero if it actually
# had boards to capture. A day that needed capturing fails loudly rather than
# quietly doing nothing.
export FOMO_LIVE_CAPTURE=1

# NOW CHECK THAT THE LINE ABOVE DID WHAT IT SAYS.
#
# THE VARIABLE'S NAME IS SPELLED OUT A SECOND TIME HERE ON PURPOSE. That
# duplication IS the check -- DO NOT TIDY IT. The obvious cleanup is to put
# the name in a shell variable and use it on both lines:
#
#     name="FOMO_LIVE_CAPTURE"; export "$name=1"; [ "${!name}" = "1" ]
#
# and that version is defeated by the exact typo it is meant to catch, because
# one misspelling flows into both sides and they agree with each other about
# the wrong name. Spelled independently, a typo on EITHER line makes the two
# disagree and the run stops here. If you find yourself removing the
# repetition, you are removing the protection.
#
# WHAT THIS BUYS AND WHAT IT DOES NOT. It is NOT what prevents live traffic:
# capture_leaderboard.py already refuses, loudly and with a nonzero exit, when
# the variable is not exactly "1". What it buys is the DIAGNOSIS. Without it,
# one mistyped character on the export line turns every 07:30 fire into the
# python's "someone armed it on purpose" message -- which sends the reader
# looking at the python and at their own environment, when the broken thing is
# this wrapper's own export. One character should not cost a whole day of
# captures AND point at the wrong file.
#
# Exact match against "1", not "is it non-empty", because that is the python's
# rule too (LIVE_CAPTURE_ARMED is an == "1" comparison, so "0" and "false" are
# disarmed). The ${VAR-default} form is needed because `set -u` would otherwise
# kill the shell on an unset variable with an error that explains nothing --
# and an unset variable is precisely what a typo on the export line produces.
# No colon in that form, on purpose: unset and exported-as-empty stay different
# diagnoses, the same distinction live_capture_env_display() makes in the
# python. The value is read into armed_value so the message below reports what
# the check actually TESTED; re-reading the variable inside the message would
# let the two disagree, and when the typo is on the check line it printed the
# baffling "is 1, expected 1".
#
# THE ONE CASE IT MISSES, stated so nobody thinks it is airtight: a caller who
# had already exported the correctly-spelled variable before invoking this
# script would satisfy this check even with a typo'd export line, because the
# value read here came from them and not from the line above. launchd is not
# such a caller: the plist's EnvironmentVariables dict sets PATH and nothing
# else, so the 07:30 fire -- the path that actually matters -- is covered.
armed_value="${FOMO_LIVE_CAPTURE-<unset>}"
if [ "$armed_value" != "1" ]; then
    say "FATAL: the export line in this wrapper did not arm the capture."
    say "FATAL: FOMO_LIVE_CAPTURE reads as \"$armed_value\", expected \"1\"."
    say "FATAL: The bug is in run_capture.sh: either the export line or the"
    say "FATAL: armed_value check below it spells the variable's name wrong,"
    say "FATAL: and the two no longer agree. Nothing was run -- no browser,"
    say "FATAL: no capture, no manifest row."
    say "==== fomo capture: finished exit=$EXIT_NOT_ARMED elapsed=0s ===="
    exit "$EXIT_NOT_ARMED"
fi
say "live capture armed (FOMO_LIVE_CAPTURE=1, verified)"

# caffeinate -i: assert "do not idle-sleep" for as long as the capture runs.
# The board does not render headless (measured -- see launch_browser()), so
# this is a real browser doing real work on a real display for five and a half
# minutes, and an idle timer or a closed lid part way through would suspend it
# mid-capture. -i is the narrow flag: it prevents IDLE sleep only, and does not
# stop the user closing the lid or the machine sleeping for any other reason.
"$CAFFEINATE" -i "$PYTHON" "$CAPTURE_SCRIPT" capture &
runner_pid=$!
say "capture running as pid $runner_pid, timeout ${TIMEOUT_SECONDS}s"

# THE WATCHDOG. A subshell that sleeps out the budget and then kills the tree.
# Written this way rather than as a poll-and-`kill -0` loop because a finished
# background process stays a zombie until the shell reaps it, and `kill -0`
# succeeds on a zombie -- a polling watchdog can therefore sit there watching a
# process that has already exited. `wait` has no such ambiguity.
( "$SLEEP" "$TIMEOUT_SECONDS"; kill_tree "$runner_pid" ) &
watchdog_pid=$!

wait "$runner_pid"
exit_code=$?
elapsed=$(( $("$DATE" +%s) - started_epoch ))

# Elapsed time is what distinguishes "the watchdog killed it" from "it failed
# on its own", because a killed process reports a signal exit, not a reason.
# The honest caveat: a run that ended of its own accord in the same second the
# watchdog fired would be reported as a timeout. The log tells them apart --
# a real timeout has no "capture run finished" line from the python above it.
if [ "$elapsed" -ge "$TIMEOUT_SECONDS" ]; then
    # THE WATCHDOG IS STILL WORKING. `wait` above returned the moment the
    # runner died of the watchdog's SIGTERM, which is part way through
    # kill_tree -- before the grace period and before the SIGKILL pass. So
    # wait for the watchdog to FINISH. Measured while testing this script:
    # killing the watchdog here instead (which is what the clean path below
    # does, correctly, to a watchdog that is merely sleeping) cancels the
    # SIGKILL pass and leaves anything that ignored SIGTERM -- Chrome, most
    # likely -- running with nothing left watching it. That is precisely the
    # abandoned browser this whole teardown exists to prevent.
    wait "$watchdog_pid" 2>/dev/null
    watchdog_pid=0
    say "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    say "!! TIMED OUT after ${elapsed}s (budget ${TIMEOUT_SECONDS}s)."
    say "!! The capture was KILLED, browser included. Today's run is"
    say "!! incomplete: boards with no manifest row were never attempted."
    say "!! Check captures.csv for today, and check for a stuck Chrome."
    say "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    say "==== fomo capture: finished exit=$EXIT_TIMED_OUT elapsed=${elapsed}s ===="
    exit "$EXIT_TIMED_OUT"
fi

# The run finished on its own, so the watchdog is still asleep on its budget.
# Stop it before it can wake up and kill a pid that has been reaped and reused
# by something else entirely.
stop_watchdog

say "==== fomo capture: finished exit=$exit_code elapsed=${elapsed}s ===="
exit "$exit_code"
