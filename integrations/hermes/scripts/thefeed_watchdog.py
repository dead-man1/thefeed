#!/usr/bin/env python3
# no_agent cron entry: Hermes cron can't pass args to a script, so this wrapper does it.
import os, runpy, sys
sys.argv = ["thefeed_bridge.py", "watchdog"]
runpy.run_path(os.path.join(os.path.dirname(os.path.abspath(__file__)), "thefeed_bridge.py"), run_name="__main__")
