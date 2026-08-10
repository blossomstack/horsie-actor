#!/usr/bin/env bash
# The README's example block must be `examples/counter.rs` verbatim, from its
# first `use` line down. The README says it is, and a README that lies about
# compiling is worse than one with no example at all — this is what makes the
# claim true rather than aspirational.
#
# Pass `--fix` to rewrite the README from the example instead of failing.
set -euo pipefail
cd "$(dirname "$0")/.."

want=$(sed -n '/^use /,$p' examples/counter.rs)

# The first ```rust block in the README, without its fences.
have=$(awk '/^```rust$/{f=1;next} f&&/^```$/{exit} f' README.md)

if [ "$want" = "$have" ]; then
  echo "README example matches examples/counter.rs"
  exit 0
fi

if [ "${1:-}" = "--fix" ]; then
  python3 - "$want" <<'PY'
import re, sys
want = sys.argv[1]
readme = open("README.md").read()
readme = re.sub(r"```rust\n.*?\n```", "```rust\n" + want + "\n```", readme, count=1, flags=re.S)
open("README.md", "w").write(readme)
PY
  echo "README example rewritten from examples/counter.rs"
  exit 0
fi

echo "error: the README example has drifted from examples/counter.rs" >&2
diff <(echo "$have") <(echo "$want") >&2 || true
echo >&2
echo "run ./scripts/check-readme-example.sh --fix" >&2
exit 1
