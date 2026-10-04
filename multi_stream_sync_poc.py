import asyncio
import os
import re
import time
import sys
import subprocess

# Sensitivity threshold for world-feed camera cuts (0.32 - 0.40)
SCENE_THRESHOLD = 0.35
# Duration in seconds to sample (90s ensures overlapping cuts even with large delays)
CAPTURE_DURATION = 90

SCENE_REGEX = re.compile(r"lavfi\.scene_score=([0-9\.]+)")
PTS_REGEX = re.compile(r"pts_time:([0-9\.]+)")

async def monitor_stream_cuts(stream_label: str, stream_url: str, cuts_dict: dict, status_dict: dict):
    """
    Connects to a single stream, captures camera cuts, and actively monitors
    for connection failures or crashes to prevent pipe deadlocks.
    """
    video_filter = (
        f"crop=in_w:in_h*0.60:0:in_h*0.20,"
        f"scale=320:-1,"
        f"select='gt(scene,{SCENE_THRESHOLD})',"
        f"metadata=print:key=lavfi.scene_score:file=-"
    )

    cmd = [
        "ffmpeg",
        "-re",
        "-nostdin",
        "-hide_banner",
        "-loglevel", "error",
        "-rw_timeout", "5000000",
        "-i", stream_url,
        "-vf", video_filter,
        "-an", "-sn",
        "-fps_mode", "vfr",
        "-t", str(CAPTURE_DURATION),
        "-f", "null",
        "-"
    ]

    cuts_dict[stream_label] = []
    status_dict[stream_label] = "Running"
    error_log = []

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        async def consume_stderr():
            async for err_line in proc.stderr:
                decoded_err = err_line.decode("utf-8", errors="ignore").strip()
                if decoded_err:
                    error_log.append(decoded_err)

        stderr_task = asyncio.create_task(consume_stderr())
        last_pts = None

        while True:
            line = await proc.stdout.readline()
            if not line:
                break

            decoded = line.decode("utf-8", errors="ignore").strip()

            pts_match = PTS_REGEX.search(decoded)
            if pts_match:
                last_pts = float(pts_match.group(1))

            scene_match = SCENE_REGEX.search(decoded)
            if scene_match:
                arrival_time = time.time()
                score = float(scene_match.group(1))
                cuts_dict[stream_label].append({
                    "wall_clock": arrival_time,
                    "pts": last_pts,
                    "score": score
                })
                print(f"[{stream_label}] Cut observed at {time.strftime('%H:%M:%S', time.localtime(arrival_time))} "
                      f"(Score: {score:.2f})")

        await proc.wait()
        await stderr_task

        if proc.returncode != 0:
            crash_reason = error_log[-1] if error_log else f"Exited with code {proc.returncode}"
            crash_reason = re.sub(r"\[.*?\]\s*", "", crash_reason)
            status_dict[stream_label] = f"Failed: {crash_reason}"
        elif len(cuts_dict[stream_label]) == 0:
            status_dict[stream_label] = "No cuts detected"
        else:
            status_dict[stream_label] = "Success"

    except asyncio.CancelledError:
        if proc.returncode is None:
            proc.kill()
    except Exception as exc:
        status_dict[stream_label] = f"Exception: {exc}"


async def rank_all_streams(stream_inputs: dict[str, str]):
    cuts_data = {}
    status_data = {}

    print(f"Starting {CAPTURE_DURATION}s capture across {len(stream_inputs)} candidate streams...")
    print("Top and bottom 20% graphics regions cropped out. Listening for world-feed cuts...\n")

    tasks = [
        asyncio.create_task(monitor_stream_cuts(label, url, cuts_data, status_data))
        for label, url in stream_inputs.items()
    ]

    await asyncio.gather(*tasks)

    print("\n==================================================")
    print("                 LATENCY RANKING                  ")
    print("==================================================")

    valid_streams = {k: v for k, v in cuts_data.items() if len(v) >= 2 and status_data[k] == "Success"}
    failed_streams = {k: status_data[k] for k in stream_inputs.keys() if status_data[k].startswith("Failed") or status_data[k].startswith("Exception")}
    nocut_streams = {k: status_data[k] for k in stream_inputs.keys() if status_data[k] == "No cuts detected" or (status_data[k] == "Success" and len(cuts_data[k]) < 2)}

    final_rankings = []

    if len(valid_streams) < 2:
        print("Not enough camera cuts detected to calculate a reliable pattern match.")
        print("You need at least 2 streams with 2+ cuts each. Try running during more active play.")
    else:
        reference_label = list(valid_streams.keys())[0]
        reference_cuts = [c["wall_clock"] for c in valid_streams[reference_label]]
        delays = {reference_label: 0.0}

        for label in valid_streams:
            if label == reference_label:
                continue

            candidate_cuts = [c["wall_clock"] for c in valid_streams[label]]
            best_match_count = 0
            best_offset = 0.0

            for ref_cut in reference_cuts:
                for cand_cut in candidate_cuts:
                    test_offset = cand_cut - ref_cut
                    matches = 0
                    for c_test in candidate_cuts:
                        for r_test in reference_cuts:
                            if abs((c_test - test_offset) - r_test) < 1.0:
                                matches += 1

                    if matches > best_match_count:
                        best_match_count = matches
                        best_offset = test_offset

            delays[label] = best_offset

        fastest_label = min(delays, key=delays.get)
        fastest_offset = delays[fastest_label]

        for label, raw_offset in delays.items():
            delay_behind_leader = raw_offset - fastest_offset
            final_rankings.append((label, delay_behind_leader))

        final_rankings.sort(key=lambda x: x[1])

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

    # --- AUTO LAUNCH VLC ---
    if final_rankings:
        print("\nLaunching ranked streams in VLC for visual confirmation...")
        for rank, (label, delay) in enumerate(final_rankings, start=1):
            url = stream_inputs[label]
            delay_str = "0.00s" if rank == 1 else f"+{delay:.2f}s"

            # This text will permanently display on the video itself and in the window title
            title_text = f"Rank {rank} | {label} | {delay_str}"

            vlc_cmd = [
                "vlc",
                "--no-one-instance",           # Forces a new window for each stream
                "--meta-title", title_text,    # Changes the window title
                "--sub-source", "marq",        # Enables text overlay
                "--marq-marquee", title_text,  # The text to display
                "--marq-position", "5",        # Position: 5 = Top Left
                "--marq-size", "24",           # Font size
                "--marq-color", "16776960",    # Hex color for Yellow
                url
            ]

            try:
                # Launch in background and suppress terminal output from VLC
                subprocess.Popen(vlc_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                print(f"Launched {label} in VLC.")
            except FileNotFoundError:
                print("\nError: VLC is not installed or not available in PATH. Cannot auto-launch.")
                break


if __name__ == "__main__":
    stream_map = {}
    file_path = "streams.txt"

    if os.path.exists(file_path):
        print(f"Found {file_path}. Loading URLs...")
        with open(file_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]

        if not lines:
            print(f"Error: {file_path} is empty.")
            sys.exit(1)

        for idx, url in enumerate(lines, start=1):
            stream_map[f"Stream_{idx}"] = url
    elif len(sys.argv) >= 2:
        print("No streams.txt found. Falling back to command-line arguments...")
        for idx, url in enumerate(sys.argv[1:], start=1):
            stream_map[f"Stream_{idx}"] = url
    else:
        print("Error: No streams provided.")
        print("Please create a 'streams.txt' file with one URL per line, or pass URLs as arguments.")
        sys.exit(1)

    try:
        asyncio.run(rank_all_streams(stream_map))
    except KeyboardInterrupt:
        print("\nTest cancelled by user.")
