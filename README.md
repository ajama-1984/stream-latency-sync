# ⚠️ Proof of Concept: Stream Latency Sync (Independent Utility for M3U Streams)

An experimental Python and FFmpeg utility designed to measure **true live stream latency** by tracking and pattern-matching shared world-feed camera cuts across candidate streams. 

---

## 🛑 Disclaimer, Status & Scope
* **Status:** **Proof of Concept (PoC) / Early Testing.** 
* **Nature of the Tool:** This is an independent, standalone sidecar script. **It is not a native Dispatcharr plugin or extension.** It does not hook into Dispatcharr's internal code, database, or API; it simply accepts the same raw M3U stream URLs that you use in your media stack.
* **Use at Your Own Risk:** The authors and contributors assume no liability for performance impacts, resource spikes, or provider interactions resulting from running this script.

---

## The Problem
Existing homelab media automation stacks typically score and sort streams based on:
1. **Static text tokens** (e.g., "FHD", "50fps", provider name groups).
2. **Physical health metrics** (bitrate, uptime, and black-screen detection).

While effective for picture quality and availability, **this leaves the stack completely blind to real-world latency**. A stream awarded top priority for its premium naming convention can physically sit **30+ seconds behind** a generically named feed, breaking the live sports experience (e.g., receiving goal notifications on your phone long before seeing them on screen).

---

## Core Methodology
To bypass proprietary broadcaster graphics and language barriers, the script evaluates the physical visual feed itself:

1. **Graphic Isolation (The "Centre-Cut"):** Broadcasters use proprietary scorebugs at the top of the screen and scrolling news tickers or studio graphics at the bottom. An FFmpeg crop filter (`crop=in_w:in_h*0.60:0:in_h*0.20`) isolates the middle 60% of the frame, entirely discarding top and bottom noise.
2. **Lightweight Probing:** The cropped video is downscaled to 320p (`scale=320:-1`), keeping CPU overhead negligible even when running concurrent streams.
3. **World-Feed Cut Detection:** During active match play, international networks share the identical director feed. Hard camera cuts (e.g., switching from wide tactical view to a player close-up) generate a massive pixel delta at the exact same physical instant. FFmpeg's `scene` filter detects these cuts.
4. **Pattern Cross-Matching:** Over a 90-second capture window, the script logs wall-clock cut timestamps and cross-matches the candidate streams against a reference stream to calculate the exact relative time offset.
5. **VLC Visual Verification:** Upon completion, the script automatically spawns candidate streams in VLC in ranked order, injecting large yellow marquee overlays (`--sub-source marq`) displaying their exact delay behind the leader (e.g., `0.00s` vs `+29.40s`).

---

## Requirements
* Python 3.8+
* FFmpeg installed and accessible in your system `PATH`.
* VLC Media Player (optional, required only for automatic visual launcher verification).

---

## Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/YOUR_USERNAME/stream-latency-sync.git](https://github.com/YOUR_USERNAME/stream-latency-sync.git)
   cd stream-latency-sync
   ```
Create your target stream list:
Create a file named streams.txt in the root directory and add your candidate stream URLs (one per line). Lines starting with # are treated as comments.

Plaintext
# Candidate streams for testing
http://your-provider-url/live/user/pass/1234.ts
http://your-provider-url/live/user/pass/5678.ts


Usage
Run the script directly via your terminal:

Bash
python multi_stream_sync_poc.py
Alternatively, you can pass stream URLs directly as command-line arguments:

Bash
python multi_stream_sync_poc.py [http://url1.ts](http://url1.ts) [http://url2.ts](http://url2.ts)
Example Output
Plaintext
Starting 90s capture across 4 candidate streams...
Top and bottom 20% graphics regions cropped out. Listening for world-feed cuts...

[Stream_1] Cut observed at 15:02:10 (Score: 0.41)
[Stream_2] Cut observed at 15:01:40 (Score: 0.39)
...

==================================================
                 LATENCY RANKING                  
==================================================
🥇 LEADER: Stream_2 (Most current feed)

Rank  Stream              Delay Behind Leader 
--------------------------------------------------
1     Stream_2            0.00s (Fastest)     
      └─ http://...
2     Stream_1            +30.12s             
      └─ http://...

Launching ranked streams in VLC for visual confirmation...
Contributing & Community Discussion
This is an open-source proof of concept intended to foster community discussion around latency-aware stream sorting. Contributions, bug reports, and discussions regarding edge-case handling (such as half-time studio segments or independent broadcaster feeds) are welcome via GitHub Issues and Discussions.

Licence
Distributed under the MIT Licence. See LICENSE for more information.
