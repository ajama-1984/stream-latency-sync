import asyncio
import os
import re
import time
import sys
import subprocess

# ==============================================================================
# CONFIGURATION CONSTANTS
# ==============================================================================

# Sensitivity threshold for world-feed camera cuts (0.32 - 0.40 is ideal).
# A score of 0.35 means 35% of the pixels must drastically change between two 
# consecutive frames to register as a hard director cut.
SCENE_THRESHOLD = 0.35

# Duration in seconds to sample the streams. 
# 90 seconds ensures that even if a stream is delayed by 45 seconds, there is 
# still a 45-second window of overlapping footage to pattern-match camera cuts.
CAPTURE_DURATION = 90

# Regular expressions to parse FFmpeg's metadata output from the scene filter.
SCENE_REGEX = re.compile(r"lavfi\.scene_score=([0-9\.]+)")
PTS_REGEX = re.compile(r"pts_time:([0-9\.]+)")


async def monitor_stream_cuts(stream_label: str, stream_url: str, cuts_dict: dict, status_dict: dict):
    """
    Connects to a single live stream using FFmpeg, isolates the centre of the picture,
    and logs the exact wall-clock time whenever a camera cut is detected.
    
    This runs asynchronously to prevent pipe deadlocks and allow multiple streams 
    to be monitored concurrently without blocking the main thread.
    """
    
    # --------------------------------------------------------------------------
    # FFMPEG VIDEO FILTER CHAIN
    # --------------------------------------------------------------------------
    # 1. crop: Keeps the middle 60% of vertical height, starting 20% down from the top.
    #    This removes the top scorebug AND the bottom news ticker/studio graphics.
    # 2. scale: Downscales the remaining cropped video to 320p to minimise CPU usage.
    # 3. select: Only outputs frames that exceed our SCENE_THRESHOLD (hard cuts).
    # 4. metadata: Prints the mathematical scene score to the standard output.
    video_filter = (
        f"crop=in_w:in_h*0.60:0:in_h*0.20,"
        f"scale=320:-1,"
        f"select='gt(scene,{SCENE_THRESHOLD})',"
        f"metadata=print:key=lavfi.scene_score:file=-"
    )

    # --------------------------------------------------------------------------
    # FFMPEG COMMAND BUILDER
    # --------------------------------------------------------------------------
    cmd = [
        "ffmpeg",
        "-re",                           # Read input at native frame rate
        "-nostdin",                      # Disable standard input interaction
        "-hide_banner",                  # Suppress FFmpeg build information
        "-loglevel", "error",            # Only output critical errors to stderr
        "-rw_timeout", "5000000",        # 5-second timeout for stalled network connections
        "-i", stream_url,                # The input stream URL
        "-vf", video_filter,             # Apply the filter chain defined above
        "-an", "-sn",                    # Drop audio and subtitle streams to save CPU
        "-fps_mode", "vfr",              # Variable framerate (only outputs the cut frames)
        "-t", str(CAPTURE_DURATION),     # Stop processing after this many seconds
        "-f", "null",                    # Discard the actual video output (we only want logs)
        "-"                              # Send output to pipe
    ]

    # Initialise dictionaries to track this stream's progress
    cuts_dict[stream_label] = []
    status_dict[stream_label] = "Running"
    error_log = []

    try:
        # Launch FFmpeg as an asynchronous subprocess
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        # Helper function to continuously read stderr to catch crashes without blocking
        async def consume_stderr():
            async for err_line in proc.stderr:
                decoded_err = err_line.decode("utf-8", errors="ignore").strip()
                if decoded_err:
                    error_log.append(decoded_err)

        stderr_task = asyncio.create_task(consume_stderr())
        last_pts = None

        # Continuously read FFmpeg's standard output line by line
        while True:
            line = await proc.stdout.readline()
            if not line:
                break # FFmpeg has finished processing or the connection dropped

            decoded = line.decode("utf-8", errors="ignore").strip()

            # Extract the Presentation Timestamp (PTS) of the frame
            pts_match = PTS_REGEX.search(decoded)
            if pts_match:
                last_pts = float(pts_match.group(1))

            # Extract the scene change score
            scene_match = SCENE_REGEX.search(decoded)
            if scene_match:
                arrival_time = time.time()
                score = float(scene_match.group(1))
                
                # Log the cut's exact arrival time on the local machine's clock
                cuts_dict[stream_label].append({
                    "wall_clock": arrival_time,
                    "pts": last_pts,
                    "score": score
                })
                print(f"[{stream_label}] Cut observed at {time.strftime('%H:%M:%S', time.localtime(arrival_time))} "
                      f"(Score: {score:.2f})")

        # Wait for both the FFmpeg process and the stderr consumer to finish
        await proc.wait()
        await stderr_task

        # Evaluate the exit status to determine if the capture was successful
        if proc.returncode != 0:
            crash_reason = error_log[-1] if error_log else f"Exited with code {proc.returncode}"
            crash_reason = re.sub(r"\[.*?\]\s*", "", crash_reason)
            status_dict[stream_label] = f"Failed: {crash_reason}"
        elif len(cuts_dict[stream_label]) == 0:
            status_dict[stream_label] = "No cuts detected"
        else:
            status_dict[stream_label] = "Success"

    except asyncio.CancelledError:
        # Ensure the process is forcefully killed if the main script is interrupted
        if proc.returncode is None:
            proc.kill()
    except Exception as exc:
        status_dict[stream_label] = f"Exception: {exc}"


async def rank_all_streams(stream_inputs: dict[str, str]):
    """
    Manages the concurrent execution of all stream monitors, evaluates the collected
    cut timestamps to find pattern matches, and calculates the final latency rankings.
    """
    cuts_data = {}
    status_data = {}
    
    print(f"Starting {CAPTURE_DURATION}s capture across {len(stream_inputs)} candidate streams...")
    print("Top and bottom 20% graphics regions cropped out. Listening for world-feed cuts...\n")

    # Spawn an asynchronous task for every stream in the input list
    tasks = [
        asyncio.create_task(monitor_stream_cuts(label, url, cuts_data, status_data))
        for label, url in stream_inputs.items()
    ]

    # Run all FFmpeg processes concurrently and wait for them to finish
    await asyncio.gather(*tasks)

    print("\n==================================================")
    print("                 LATENCY RANKING                  ")
    print("==================================================")

    # Filter streams into categories based on their success/failure status
    valid_streams = {k: v for k, v in cuts_data.items() if len(v) >= 2 and status_data[k] == "Success"}
    failed_streams = {k: status_data[k] for k in stream_inputs.keys() if status_data[k].startswith("Failed") or status_data[k].startswith("Exception")}
    nocut_streams = {k: status_data[k] for k in stream_inputs.keys() if status_data[k] == "No cuts detected" or (status_data[k] == "Success" and len(cuts_data[k]) < 2)}

    final_rankings = []
    
    if len(valid_streams) < 2:
        print("Not enough camera cuts detected to calculate a reliable pattern match.")
        print("You need at least 2 streams with 2+ cuts each. Try running during more active play.")
    else:
        # ----------------------------------------------------------------------
        # PATTERN MATCHING & LATENCY CALCULATION
        # ----------------------------------------------------------------------
        # We designate the first valid stream as our 'reference' to compare against.
        reference_label = list(valid_streams.keys())[0]
        reference_cuts = [c["wall_clock"] for c in valid_streams[reference_label]]
        delays = {reference_label: 0.0}

        # Compare every other valid stream against the reference stream
        for label in valid_streams:
            if label == reference_label:
                continue
                
            candidate_cuts = [c["wall_clock"] for c in valid_streams[label]]
            best_match_count = 0
            best_offset = 0.0

            # Slide the candidate stream's timeline against the reference timeline.
            # We test the offset between every single cut combination to find the 
            # alignment that yields the highest number of matching cuts within a 1s margin.
            for ref_cut in reference_cuts:
                for cand_cut in candidate_cuts:
                    test_offset = cand_cut - ref_cut
                    matches = 0
                    
                    for c_test in candidate_cuts:
                        for r_test in reference_cuts:
                            # If applying the offset aligns two cuts within 1 second, it's a match
                            if abs((c_test - test_offset) - r_test) < 1.0:
                                matches += 1
                                
                    # If this offset produces the most matches so far, save it
                    if matches > best_match_count:
                        best_match_count = matches
                        best_offset = test_offset

            delays[label] = best_offset

        # The stream with the lowest numerical offset (most negative or zero) is the leader
        fastest_label = min(delays, key=delays.get)
        fastest_offset = delays[fastest_label]

        # Normalise all delays so the leader is exactly 0.00s
        for label, raw_offset in delays.items():
            delay_behind_leader = raw_offset - fastest_offset
            final_rankings.append((label, delay_behind_leader))

        # Sort the final output from fastest to slowest
        final_rankings.sort(key=lambda x: x[1])

        # ----------------------------------------------------------------------
        # TERMINAL OUTPUT FORMATTING
        # ----------------------------------------------------------------------
        print(f"🥇 LEADER: {fastest_label} (Most current feed)\n")
        print(f"{'Rank':<6}{'Stream':<20}{'Delay Behind Leader':<20}")
        print("-" * 50)

        for rank, (label, delay) in enumerate(final_rankings, start=1):
            url = stream_inputs[label]
            if rank == 1:
                print(f"{rank:<6}{label:<20}{'0.00s (Fastest)':<20}")
                print(f"      └─ {url}")
            else:
                print(f"{rank:<6}{label:<20}{f'+{delay:.2f}s':<20}")
                print(f"      └─ {url}")

    # Output diagnostics for streams that failed or lacked sufficient data
    if failed_streams:
        print("\n--------------------------------------------------")
        print("❌ DEAD / FAILED STREAMS:")
        for label, reason in failed_streams.items():
            print(f"{label}: {reason}")
            print(f"      └─ {stream_inputs[label]}")

    if nocut_streams:
        print("\n--------------------------------------------------")
        print("⚠️ CONNECTED, BUT INSUFFICIENT CUTS DETECTED:")
        for label, reason in nocut_streams.items():
            reason_text = "No cuts detected" if status_data[label] == "No cuts detected" else "Caught <2 cuts (need 2 for pattern match)"
            print(f"{label}: {reason_text}")
            print(f"      └─ {stream_inputs[label]}")

    # --------------------------------------------------------------------------
    # AUTOMATIC VISUAL VERIFICATION (VLC LAUNCHER)
    # --------------------------------------------------------------------------
    if final_rankings:
        print("\nLaunching ranked streams in VLC for visual confirmation...")
        for rank, (label, delay) in enumerate(final_rankings, start=1):
            url = stream_inputs[label]
            delay_str = "0.00s" if rank == 1 else f"+{delay:.2f}s"
            
            # This text will permanently display on the video itself and in the window title
            title_text = f"Rank {rank} | {label} | {delay_str}"
            
            # Command arguments to launch VLC, force a new window, and inject a marquee overlay
            vlc_cmd = [
                "vlc",
                "--no-one-instance",           # Forces a new discrete window for this specific stream
                "--meta-title", title_text,    # Changes the operating system window title
                "--sub-source", "marq",        # Enables the built-in text overlay filter
                "--marq-marquee", title_text,  # The actual text to display on the screen
                "--marq-position", "5",        # Overlay Position: 5 equates to Top Left
                "--marq-size", "24",           # Overlay font size
                "--marq-color", "16776960",    # Hexadecimal colour code for Yellow
                url
            ]
            
            try:
                # Launch the VLC process in the background. 
                # DEVNULL suppresses VLC's verbose terminal spam so our rankings remain readable.
                subprocess.Popen(vlc_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                print(f"Launched {label} in VLC.")
            except FileNotFoundError:
                print("\nError: VLC is not installed or not available in PATH. Cannot auto-launch.")
                break


if __name__ == "__main__":
    stream_map = {}
    file_path = "streams.txt"

    # Attempt to load streams from a local text file first
    if os.path.exists(file_path):
        print(f"Found {file_path}. Loading URLs...")
        # Read lines, stripping whitespace and ignoring lines that start with '#'
        with open(file_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
        
        if not lines:
            print(f"Error: {file_path} is empty.")
            sys.exit(1)
            
        for idx, url in enumerate(lines, start=1):
            stream_map[f"Stream_{idx}"] = url
            
    # Fallback: check if streams were provided directly as command-line arguments
    elif len(sys.argv) >= 2:
        print("No streams.txt found. Falling back to command-line arguments...")
        for idx, url in enumerate(sys.argv[1:], start=1):
            stream_map[f"Stream_{idx}"] = url
            
    # If neither method provides URLs, terminate the script
    else:
        print("Error: No streams provided.")
        print("Please create a 'streams.txt' file with one URL per line, or pass URLs as arguments.")
        sys.exit(1)

    # Execute the main asynchronous routine, allowing clean exit via Ctrl+C
    try:
        asyncio.run(rank_all_streams(stream_map))
    except KeyboardInterrupt:
        print("\nTest cancelled by user.")
