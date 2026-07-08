see the issue. The lockfile and registry URLs look correct (:2242), but npm ci is likely still hitting the ENOTEMPTY error even on /tmp. The Databricks Apps build container may not use tmpfs for /tmp. Let me switch to a more robust approach using npm install --force with explicit no-optional (to skip native packages like lz4 that need compilation):


  - sh
  - -c
  - |
    set -euo pipefail
  export NPM_CONFIG_CACHE="/tmp/npm-cache-$$"
  mkdir -p "$NPM_CONFIG_CACHE"
  rm -rf node_modules
  find . -maxdepth 1 -name '.apache-arrow-*' -exec rm -rf {} + 2>/dev/null || true
  npm install --force --no-optional --no-audit --no-fund
  npm run build
  exec npm run start

Jul 7, 2026, 04:06:00 PM CDT	BUILD	...5f1d17d	[ERROR] Error installing packages. Please check /logz for more details
Jul 7, 2026, 04:06:00 PM CDT	BUILD	...5f1d17d	[INFO] Deployment 01f17a472efc153db1e37d4405f1d17d ended in 2m55.379824781s
Jul 7, 2026, 04:06:00 PM CDT	BUILD	...5f1d17d	npm error A complete log of this run can be found in: /home/app/.npm/_logs/2026-07-07T21_03_19_245Z-debug-0.log
Jul 7, 2026, 04:06:00 PM CDT	BUILD	...5f1d17d	npm verbose code 1
Jul 7, 2026, 04:06:00 PM CDT	BUILD	...5f1d17d	npm verbose exit 1
Jul 7, 2026, 04:06:00 PM CDT	BUILD	...5f1d17d	npm error   <https://github.com/npm/cli/issues>
Jul 7, 2026, 04:06:00 PM CDT	BUILD	...5f1d17d	npm error This is an error with npm itself. Please report this error at:
Jul 7, 2026, 04:06:00 PM CDT	BUILD	...5f1d17d	npm error Exit handler never called!
Jul 7, 2026, 04:06:00 PM CDT	BUILD	...5f1d17d	
 
npm notice npm notice New major version of npm available! 10.9.2 -> 11.18.0 npm notice Changelog: https://github.com/npm/cli/releases/tag/v11.18.0 npm notice To update run: npm install -g npm@11.18.0 npm notice
