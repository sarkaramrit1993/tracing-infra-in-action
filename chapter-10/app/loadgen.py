"""Load generator.

No arguments: steady background, two /cart requests a second, forever.
--count N: the measured run. Requests i = 0 .. N-1 against /checkout, paced to
--rate per second, then one line of ground truth on stdout.
"""

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request

BASE = "http://checkout-service:8080"


def get(url):
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            resp.read()
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except OSError:
        return 0


def steady():
    while True:
        get(f"{BASE}/cart")
        time.sleep(0.5)


def measured(count, rate, workers):
    lock = threading.Lock()
    state = {"next": 0, "errors": 0, "transport": 0}
    start = time.time()

    def work():
        while True:
            with lock:
                i = state["next"]
                if i >= count:
                    return
                state["next"] += 1
            wait = start + i / rate - time.time()
            if wait > 0:
                time.sleep(wait)
            status = get(f"{BASE}/checkout?i={i}")
            if status == 500:
                with lock:
                    state["errors"] += 1
            elif status != 200:
                with lock:
                    state["transport"] += 1

    threads = [threading.Thread(target=work) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - start
    print(json.dumps({"requests": count, "errors": state["errors"],
                      "transport_failures": state["transport"],
                      "seconds": round(elapsed, 1),
                      "achieved_rate": round(count / elapsed, 1)}))
    return 0 if state["transport"] == 0 else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int)
    ap.add_argument("--rate", type=float, default=200.0)
    ap.add_argument("--workers", type=int, default=32)
    a = ap.parse_args()
    if a.count is None:
        steady()
    sys.exit(measured(a.count, a.rate, a.workers))
