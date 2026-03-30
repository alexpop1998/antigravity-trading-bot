import re
import traceback

log_file = "start_output.log"
with open(log_file, "r") as f:
    content = f.read()

# find traceback for ADA/USDT:USDT
tb = re.split(r'FATAL error profiling', content)
if len(tb) > 1:
    print(tb[1][:500])
