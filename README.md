# Global AI Venture Capital Intelligence Dashboard

A real-time intelligence dashboard that tracks AI and data startup funding across 25 nations, powered by Google Gemini and deployed on GitHub Pages.

**Live dashboard:** Deployed via GitHub Pages from this repository.

**Created by [Mohit Chhaparia](https://www.linkedin.com/in/mohit-chhaparia/)** | #DataBytesWithMo

---

## How It Works

The system spans two repositories:

| Repository | Visibility | Purpose |
|-----------|-----------|---------|
| **AI-Startup-Funding-Radar** | Private | Data collection, storage, email notifications |
| **This repository** | Public | Dashboard frontend, deployed to GitHub Pages |

### Data Collection (Private Repo)

The private repository runs automated workflows that use **Google Gemini 2.5 Flash** with Google Search grounding to scan venture capital news sources across 25 nations, 3 times per day.

**`automation.py`** is the core data collection script. For each nation, it:
1. Constructs a prompt with nation-specific sources and search instructions
2. Calls Gemini with `google_search` tool enabled for real-time web data
3. Parses the JSON response into structured deal records
4. Deduplicates against existing history in `data/{Nation}.json`
5. Appends new deals with a `Date_Captured` timestamp

**`send_notification.py`** sends a daily digest email at 7:30 AM CT with all deals captured in the previous 24 hours, grouped by tier and attached as a CSV.

**`nation_config.json`** defines the 25 tracked nations across 5 tiers, each with curated news sources, regional hubs to monitor, and search guidance.

### Workflow Schedule (Private Repo)

The private repository uses a **15-time rotating dispatcher** (`dispatcher.yml`) that routes work into five reusable workflows (`list1.yml` through `list5.yml`).

- The dispatcher runs in **three daily blocks**
- Each block has **five trigger slots**
- The selected workflow is based on:
  - the UTC day-of-month rotation: `((day - 1) % 5) + 1`
  - the slot index that fired within the current block

This creates a repeating five-day pattern:

- 1st -> schedule 1
- 2nd -> schedule 2
- 3rd -> schedule 3
- 4th -> schedule 4
- 5th -> schedule 5
- 6th -> schedule 1
- ...and so on

The dispatcher trigger times are:

| Block | UTC Trigger Times |
|-------|-------------------|
| Block 1 | 05:00, 05:30, 06:00, 06:30, 07:00 |
| Block 2 | 13:00, 13:30, 14:00, 14:30, 15:00 |
| Block 3 | 21:00, 21:30, 22:00, 22:30, 23:00 |

The public dashboard footer reflects the **expected completed sync windows** after those private workflows finish and this repository redeploys: **02:30, 10:30, and 18:30 CT**.

Each selected list workflow still uses a matrix strategy to run nations in parallel (max 5 concurrent), then commits results back to the private repo.

### Dashboard Deployment (This Repo)

When this repository's `deploy-pages.yml` workflow runs, it:
1. Checks out this repo (HTML, CSS, JS, scripts)
2. Downloads assets and data JSON files from the private repo via GitHub API
3. Generates `data/manifest.json`, `data/fx_rates.json`, and `data/outlier.json`
4. Regenerates the README FX conversion table from the latest generated FX metadata
5. Deploys the full site to GitHub Pages

The dashboard can be redeployed via:
- **Push to main** (changes to `scripts/`, `index.html`, `dashboard.js`, `styles.css`)
- **Scheduled** `deploy-pages.yml` workflow runs
- **Manual trigger** via the Actions tab

---

## Dashboard Features

### Filters
- **Nations/Countries** -- multi-select with country flags
- **Round** -- funding rounds grouped into logical categories (Seed, Series A, Series B, etc.)
- **Tier** -- nation tier (1-5)
- **LinkedIn Profile / Hiring / Careers Link** -- presence/absence filters
- **Amount Range** -- log-scale dual slider with text inputs (USD-equivalent)
- **Date Range** -- calendar date pickers
- **Search** -- full-text search across all deal fields

All filters support **cross-filtering**: selecting a value in one filter updates the available options in all other filters to show only what's relevant.

### Stats Cards
- **Overview card** -- total deals, funding, countries, average deal size, with metadata pills
- **Highlight cards** -- top round, largest deal, actively hiring count, top tier
- **Period cards** -- 7 time windows (Today, Yesterday, This Week, Last Week, This Month, Last Month, All Time) each showing deals, funding, countries, avg size, top round, hiring %, link coverage %, and amount-known %

### Data Table
- Shows deals from the **last 7 days** (rolling window from the latest capture date)
- Sortable by any column
- Displays amounts in a consistent `CURRENCY 12.5M` format
- CSV export of the displayed deals

### Accurate Metrics
Hiring and link coverage use qualified percentages to avoid misrepresentation:
- `20% known hiring` -- percentage that are confirmed hiring
- `6% known link coverage` -- percentage with at least one LinkedIn or careers link
- `57% amounts known` -- percentage of deals in that card/window with usable amount data behind the funding and average calculations

<!-- FX_RATES_TABLE:START -->
## Live FX Conversion Rates

Rates are fetched from live market data during deployment and used to normalize non-USD deal amounts into USD-equivalent values for dashboard cards and filters.

**Last Complete Update:** 2026-04-16 11:55 CT

**Last Partial Update:** No partial update recorded

**Source:** https://www.exchangerate-api.com

The most recent pipeline run refreshed every nation in the FX table with fresh live-market values.

| Nation | Currency | USD per 1 unit of local currency |
|--------|----------|----------------------------------|
| Australia | AUD | 0.71563520 |
| Brazil | BRL | 0.20024735 |
| Britain | GBP | 1.35670852 |
| Canada | CAD | 0.72775702 |
| China | CNY | 0.14636036 |
| Denmark | DKK | 0.15803778 |
| Dubai | AED | 0.27229408 |
| Finland | EUR | 1.17992986 |
| France | EUR | 1.17992986 |
| Germany | EUR | 1.17992986 |
| India | INR | 0.01070188 |
| Ireland | EUR | 1.17992986 |
| Israel | ILS | 0.33322737 |
| Japan | JPY | 0.00629340 |
| Luxembourg | EUR | 1.17992986 |
| Netherlands | EUR | 1.17992986 |
| Portugal | EUR | 1.17992986 |
| Russia | RUB | 0.01323703 |
| Singapore | SGD | 0.78663847 |
| South Korea | KRW | 0.00067784 |
| Spain | EUR | 1.17992986 |
| Switzerland | CHF | 1.27927460 |
| Taiwan | TWD | 0.03162999 |
| UAE | AED | 0.27229408 |
| USA | USD | 1.00000000 |

<!-- FX_RATES_TABLE:END -->

---

## Data Schema

Each nation's JSON file (`data/{Nation}.json`) in the private repository contains:

```json
{
  "deals": [
    {
      "Country": "Nation name",
      "Startup_Name": "Company name",
      "Description": "2-line business summary",
      "Amount": "Funding amount (USD, local currency, or mixed-currency text)",
      "Round": "Funding stage",
      "Investors": "Comma-separated investor list",
      "Founders": "Founder names",
      "LinkedIn_Profile": "URL or N/A",
      "Hiring": "Yes/No/Unknown",
      "Careers_Link": "URL or N/A",
      "Tier": "Tier N",
      "Nation": "Nation label",
      "Flag": "Flag emoji",
      "Date_Captured": "YYYY-MM-DD"
    }
  ],
  "last_updated": "YYYY-MM-DD HH:MM"
}
```

---

## Repository Structure

### This Repository (Public Dashboard)
```
index.html                  Main dashboard page
dashboard.js                All application logic
styles.css                  Styling
scripts/
  generate_manifest.py      Builds data/manifest.json and generated metadata at deploy time
  update_readme_fx_table.py Regenerates the README FX section from generated FX data
.github/workflows/
  deploy-pages.yml          GitHub Pages deployment workflow
data/
  fx_rates.json             Generated live FX rate registry
  outlier.json              Generated cumulative outlier registry
  manifest.json             Generated dashboard manifest (build-time)
  .gitkeep                  Keeps directory in git (JSONs pulled at build time)
```

### Private Repository (AI-Startup-Funding-Radar)
```
automation.py              Gemini-powered deal collection script
send_notification.py       Daily digest email sender
nation_config.json         Nation definitions (25 nations, 5 tiers)
data/
  {Nation}.json            Per-nation deal history files
  last_updated.txt         Timestamp of latest data update
.github/workflows/
  dispatcher.yml           15-time rotating workflow dispatcher
  list1.yml - list5.yml    Reusable tier/list workflows called by the dispatcher
  daily_digest.yml         Daily email digest
```

---

## Operations Notes

- The public dashboard deploy downloads the latest private data, regenerates the manifest, live FX registry, cumulative outlier registry, and the README FX section, then deploys the site to GitHub Pages.
- Dashboard amount sorting and aggregation use USD-equivalent values derived from the generated live FX registry.
- The footer `Last sync` value is rendered in **CT (24-hour format)** using the most recent available sync reference from the private data timestamp and the current public deploy generation time.

---

## Tech Stack

- **Frontend:** HTML, CSS, JavaScript
- **Data collection:** Python 3.11, Google Gemini 2.5 Flash with Google Search
- **Hosting:** GitHub Pages (static site)
- **CI/CD:** GitHub Actions
- **Email:** Gmail SMTP (Simple Mail Transfer Protocol)
