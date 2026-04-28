const MANIFEST_PATH = 'data/manifest.json';
const LAST_UPDATED_PATH = 'data/last_updated.txt';
const FX_RATES_PATH = 'data/fx_rates.json';
const UNKNOWN_LABEL = 'Unknown';
const AMOUNT_SLIDER_MAX = 1000;
const MAX_REALISTIC_DEAL_AMOUNT = 250e9;
const MIN_REALISTIC_DEAL_AMOUNT = 1;
const SYNC_TIME_ZONE = 'America/Chicago';
const SYNC_TIME_ZONE_LABEL = 'CT';
const FALLBACK_CURRENCY_TO_USD_RATE = {
    USD: 1,
    USDC: 1,
    AED: 0.2723,
    AUD: 0.66,
    CAD: 0.74,
    CNY: 0.138,
    DKK: 0.145,
    EUR: 1.09,
    GBP: 1.28,
    ILS: 0.27,
    INR: 0.012,
    JPY: 0.0067,
    KRW: 0.00069,
    RUB: 0.012,
    SEK: 0.094,
    SGD: 0.74,
    TWD: 0.031,
    ZAR: 0.053
};
const SEARCH_FIELDS = [
    'Country',
    'Nation',
    'Startup_Name',
    'Description',
    'Amount',
    'Round',
    'Investors',
    'Founders',
    'LinkedIn_Profile',
    'Hiring',
    'Careers_Link',
    'Tier',
    'Date_Captured'
];
const TABLE_COLUMNS = [
    { key: 'Nation', label: 'Nation', className: 'nation-cell' },
    { key: 'Startup_Name', label: 'Startup' },
    { key: 'Description', label: 'Description', className: 'description-cell' },
    { key: 'Amount', label: 'Amount', className: 'amount-cell' },
    { key: 'Round', label: 'Round' },
    { key: 'Investors', label: 'Investors', className: 'investors-cell' },
    { key: 'Founders', label: 'Founders' },
    { key: 'Tier', label: 'Tier' },
    { key: 'LinkedIn_Profile', label: 'LinkedIn', className: 'link-column' },
    { key: 'Hiring', label: 'Hiring' },
    { key: 'Careers_Link', label: 'Careers', className: 'link-column' },
    { key: 'Date_Captured', label: 'Date' }
];
const PERIOD_DEFINITIONS = [
    {
        key: 'current-day',
        label: 'Current Day',
        icon: 'Today',
        getRange: anchor => ({
            start: startOfUtcDay(anchor),
            end: endOfUtcDay(anchor)
        })
    },
    {
        key: 'previous-day',
        label: 'Previous Day',
        icon: 'Yesterday',
        getRange: anchor => {
            const yesterday = new Date(anchor.getTime() - (24 * 60 * 60 * 1000));
            return {
                start: startOfUtcDay(yesterday),
                end: endOfUtcDay(yesterday)
            };
        }
    },
    {
        key: 'current-week',
        label: 'Current Week',
        icon: 'This week',
        getRange: anchor => ({
            start: startOfUtcWeek(anchor),
            end: endOfUtcDay(anchor)
        })
    },
    {
        key: 'past-week',
        label: 'Past Week',
        icon: 'Previous week',
        getRange: anchor => {
            const previousWeekAnchor = new Date(anchor.getTime() - (7 * 24 * 60 * 60 * 1000));
            return {
                start: startOfUtcWeek(previousWeekAnchor),
                end: endOfUtcWeek(previousWeekAnchor)
            };
        }
    },
    {
        key: 'current-month',
        label: 'Current Month',
        icon: 'This month',
        getRange: anchor => ({
            start: startOfUtcMonth(anchor),
            end: endOfUtcDay(anchor)
        })
    },
    {
        key: 'past-month',
        label: 'Past Month',
        icon: 'Previous month',
        getRange: anchor => {
            const previousMonthAnchor = new Date(Date.UTC(anchor.getUTCFullYear(), anchor.getUTCMonth() - 1, 1));
            return {
                start: startOfUtcMonth(previousMonthAnchor),
                end: endOfUtcMonth(previousMonthAnchor)
            };
        }
    },
    {
        key: 'all-time',
        label: 'All Time',
        icon: 'Full range',
        getRange: (_anchor, deals) => {
            const dates = deals
                .map(d => d.DateValue)
                .filter(v => Number.isFinite(v));
            if (!dates.length) {
                return { start: startOfUtcDay(_anchor), end: endOfUtcDay(_anchor) };
            }
            return {
                start: startOfUtcDay(new Date(Math.min(...dates))),
                end: endOfUtcDay(new Date(Math.max(...dates)))
            };
        }
    }
];
const ROUND_GROUP_ORDER = [
    'Pre-Seed', 'Seed', 'Pre-Series A', 'Series A', 'Series B', 'Series C', 'Series D+',
    'Growth/Late Stage', 'Acceleration', 'Bridge', 'Debt', 'Grant', 'Strategic',
    'Venture/Other', 'Other', UNKNOWN_LABEL
];

const MULTI_SELECT_FILTERS = {
    nation: {
        buttonId: 'nation-select-btn',
        dropdownId: 'nation-dropdown',
        containerId: 'nation-checkboxes',
        selectAllId: 'select-all-nations',
        clearAllId: 'clear-all-nations',
        emptyLabel: 'Select Nations',
        pluralLabel: 'Nations',
        field: 'Nation',
        formatOption: value => `${getCountryFlag(value)} ${value}`
    },
    round: {
        buttonId: 'round-select-btn',
        dropdownId: 'round-dropdown',
        containerId: 'round-checkboxes',
        selectAllId: 'select-all-rounds',
        clearAllId: 'clear-all-rounds',
        emptyLabel: 'Select Rounds',
        pluralLabel: 'Rounds',
        field: 'RoundFilter'
    },
    tier: {
        buttonId: 'tier-select-btn',
        dropdownId: 'tier-dropdown',
        containerId: 'tier-checkboxes',
        selectAllId: 'select-all-tiers',
        clearAllId: 'clear-all-tiers',
        emptyLabel: 'Select Tiers',
        pluralLabel: 'Tiers',
        field: 'TierFilter'
    },
    linkedin: {
        buttonId: 'linkedin-select-btn',
        dropdownId: 'linkedin-dropdown',
        containerId: 'linkedin-checkboxes',
        selectAllId: 'select-all-linkedin',
        clearAllId: 'clear-all-linkedin',
        emptyLabel: 'Select LinkedIn Status',
        pluralLabel: 'LinkedIn Statuses',
        field: 'LinkedInFilter'
    },
    hiring: {
        buttonId: 'hiring-select-btn',
        dropdownId: 'hiring-dropdown',
        containerId: 'hiring-checkboxes',
        selectAllId: 'select-all-hiring',
        clearAllId: 'clear-all-hiring',
        emptyLabel: 'Select Hiring Status',
        pluralLabel: 'Hiring Statuses',
        field: 'HiringFilter'
    },
    careers: {
        buttonId: 'careers-select-btn',
        dropdownId: 'careers-dropdown',
        containerId: 'careers-checkboxes',
        selectAllId: 'select-all-careers',
        clearAllId: 'clear-all-careers',
        emptyLabel: 'Select Careers Status',
        pluralLabel: 'Careers Statuses',
        field: 'CareersFilter'
    }
};

const MULTI_SELECT_KEYS = ['nation', 'round', 'tier', 'linkedin', 'hiring', 'careers'];

let allDeals = [];
let filteredDeals = [];
let nations = [];
let allFilterOptions = createEmptyFilterOptions();
let filterOptions = createEmptyFilterOptions();
let selectedFilters = createEmptySelectedFilters();
let amountRange = {
    availableMin: 0,
    availableMax: 0,
    selectedMin: 0,
    selectedMax: 0,
    hasValues: false
};
let currentSort = { column: null, direction: 'asc' };
/** @type {Record<string, { col: number, dir: 'asc' | 'desc' }>} */
let insightTableSort = {};
let dashboardInitialized = false;
let currencyToUsdRate = { ...FALLBACK_CURRENCY_TO_USD_RATE };
let activePageId = 'page-1';
let selectedCountryInsight = '';
let outlierRegistry = { records: [], currently_detected_count: 0 };

const EARLY_STAGE_ROUNDS = ['Pre-Seed', 'Seed', 'Pre-Series A', 'Series A'];

function setsEqual(a, b) {
    if (!(a instanceof Set) || !(b instanceof Set) || a.size !== b.size) {
        return false;
    }
    for (const item of a) {
        if (!b.has(item)) return false;
    }
    return true;
}

function isDefaultDateRange() {
    const dateFrom = document.getElementById('date-from');
    const dateTo = document.getElementById('date-to');
    if (!dateFrom || !dateTo) return false;

    const availableDates = allDeals.map(deal => deal.DateValue).filter(Number.isFinite);
    if (!availableDates.length) {
        return !cleanString(dateFrom.value) && !cleanString(dateTo.value);
    }

    const maxDate = new Date(Math.max(...availableDates));
    const ninetyDaysAgo = new Date(maxDate.getTime() - (90 * 24 * 60 * 60 * 1000));
    return dateFrom.value === formatDateInputValue(ninetyDaysAgo) && dateTo.value === formatDateInputValue(maxDate);
}

function isDefaultAmountRange() {
    if (!amountRange.hasValues) return true;
    return amountRange.selectedMin === amountRange.availableMin
        && amountRange.selectedMax === amountRange.availableMax;
}

function isDefaultSearch() {
    const searchInput = document.getElementById('search');
    return !cleanString(searchInput ? searchInput.value : '');
}

function matchesBaselineFilters() {
    return isDefaultDateRange() && isDefaultAmountRange() && isDefaultSearch();
}

function getEarlyStageRoundSelection() {
    const preferred = allFilterOptions.round.filter(v => EARLY_STAGE_ROUNDS.includes(v));
    return new Set(preferred);
}

function isFullSelectionForKeys(keys) {
    return keys.every(key => selectedFilters[key].size === allFilterOptions[key].length);
}

function isGlobalVcLensState() {
    return isFullSelectionForKeys(MULTI_SELECT_KEYS);
}

function isEarlyStageLensState() {
    const fullOther = ['nation', 'tier', 'linkedin', 'hiring', 'careers'];
    if (!isFullSelectionForKeys(fullOther)) {
        return false;
    }
    const expectedRounds = getEarlyStageRoundSelection();
    if (!expectedRounds.size) {
        return false;
    }
    return setsEqual(selectedFilters.round, expectedRounds);
}

function isHiringFocusedLensState() {
    const fullOther = ['nation', 'tier', 'round', 'careers'];
    if (!isFullSelectionForKeys(fullOther)) {
        return false;
    }
    if (!setsEqual(selectedFilters.hiring, new Set(['Yes']))) {
        return false;
    }
    const hasPresent = allFilterOptions.linkedin.includes('Present');
    if (hasPresent) {
        return setsEqual(selectedFilters.linkedin, new Set(['Present']));
    }
    return selectedFilters.linkedin.size === allFilterOptions.linkedin.length;
}

function getActiveSavedViewKey() {
    if (!matchesBaselineFilters()) {
        return null;
    }
    if (isHiringFocusedLensState()) {
        return 'hiring-focused';
    }
    if (isEarlyStageLensState()) {
        return 'early-stage';
    }
    if (isGlobalVcLensState()) {
        return 'global-vc';
    }
    return null;
}

function syncSavedViewButtons() {
    const activeKey = getActiveSavedViewKey();
    document.querySelectorAll('.btn-preset').forEach(btn => {
        const preset = btn.dataset.preset || '';
        const isActive = Boolean(activeKey && preset === activeKey);
        btn.classList.toggle('btn-preset--active', isActive);
        btn.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    });
}

document.addEventListener('DOMContentLoaded', () => {
    console.log('Dashboard initializing...');
    loadData();
});

function createEmptyFilterOptions() {
    return {
        nation: [],
        round: [],
        tier: [],
        linkedin: [],
        hiring: [],
        careers: []
    };
}

function createEmptySelectedFilters() {
    return {
        nation: new Set(),
        round: new Set(),
        tier: new Set(),
        linkedin: new Set(),
        hiring: new Set(),
        careers: new Set()
    };
}

async function loadData() {
    try {
        showLoading(true);

        const manifest = await loadManifest();
        await Promise.all([loadFxRates(), loadOutlierRegistry()]);
        const nationEntries = Array.isArray(manifest.nations) ? manifest.nations : [];

        if (nationEntries.length === 0) {
            showError('No data files were listed in data/manifest.json.');
            return;
        }

        const loadedDeals = [];
        const excludedDeals = [];

        for (const entry of nationEntries) {
            const nationName = cleanString(entry.name) || cleanString(entry.file).replace(/\.json$/i, '');
            const filePath = cleanString(entry.path) || `data/${entry.file}`;

            if (!filePath) {
                continue;
            }

            try {
                const response = await fetch(filePath, { cache: 'no-store' });

                if (!response.ok) {
                    console.error(`HTTP ${response.status} for ${filePath}`);
                    continue;
                }

                const data = await response.json();
                const deals = Array.isArray(data.deals) ? data.deals : [];

                deals.forEach(deal => {
                    const normalizedDeal = normalizeDeal(deal, nationName);

                    if (normalizedDeal.ExcludedReason) {
                        excludedDeals.push(normalizedDeal);
                        return;
                    }

                    loadedDeals.push(normalizedDeal);
                });
            } catch (error) {
                console.error(`Failed to load ${filePath}:`, error);
            }
        }

        if (loadedDeals.length === 0) {
            showError('No deals found in the data files. Please check the JSON structure.');
            return;
        }

        const dedupedDeals = dedupeDealsByFingerprint(loadedDeals);
        if (dedupedDeals.length < loadedDeals.length) {
            console.warn(
                `Removed ${loadedDeals.length - dedupedDeals.length} duplicate deal record(s) ` +
                '(same org country, startup, amount, and capture week—e.g. same deal in USA.json and UAE.json where Nation is source file).'
            );
        }
        loadedDeals.length = 0;
        loadedDeals.push(...dedupedDeals);

        if (excludedDeals.length) {
            console.warn(`Excluded ${excludedDeals.length} implausible deal record(s) during load.`, excludedDeals.map(deal => ({
                startup: deal.Startup_Name || 'Unknown',
                nation: deal.Nation,
                amount: deal.Amount,
                reason: deal.ExcludedReason
            })));
        }

        allDeals = loadedDeals;
        initializeFilterState();
        setDefaultDateRange();

        if (!dashboardInitialized) {
            setupEventListeners();
            dashboardInitialized = true;
        }

        populateAllFilters();
        initializeAmountRangeControls();
        filterDeals();
        await updateLastUpdated(manifest);
        showLoading(false);
    } catch (error) {
        console.error('Critical error loading data:', error);
        showError(`Failed to load data: ${error.message}`);
        showLoading(false);
    }
}

async function loadManifest() {
    const response = await fetch(MANIFEST_PATH, { cache: 'no-store' });

    if (!response.ok) {
        throw new Error(`Unable to load ${MANIFEST_PATH} (${response.status})`);
    }

    return response.json();
}

async function loadFxRates() {
    try {
        const response = await fetch(FX_RATES_PATH, { cache: 'no-store' });
        if (!response.ok) {
            return;
        }

        const payload = await response.json();
        const nextRates = payload && typeof payload === 'object'
            ? payload.currency_to_usd_rate
            : null;

        if (!nextRates || typeof nextRates !== 'object') {
            return;
        }

        currencyToUsdRate = {
            ...FALLBACK_CURRENCY_TO_USD_RATE,
            ...nextRates
        };
    } catch (error) {
        console.warn('Could not load live FX rates, using fallback rates.', error);
    }
}

async function loadOutlierRegistry() {
    try {
        const response = await fetch('data/outlier.json', { cache: 'no-store' });
        if (!response.ok) {
            outlierRegistry = { records: [], currently_detected_count: 0 };
            return;
        }
        const payload = await response.json();
        if (payload && typeof payload === 'object' && Array.isArray(payload.records)) {
            outlierRegistry = payload;
            return;
        }
        outlierRegistry = { records: [], currently_detected_count: 0 };
    } catch (error) {
        console.warn('Could not load outlier registry, continuing without anomaly panel.', error);
        outlierRegistry = { records: [], currently_detected_count: 0 };
    }
}

function applyFilterPreset(presetKey) {
    const preset = String(presetKey || '').toLowerCase();
    if (!preset) return;

    MULTI_SELECT_KEYS.forEach(key => {
        selectedFilters[key] = new Set(allFilterOptions[key]);
    });
    initializeAmountRange();
    initializeAmountRangeControls();
    setDefaultDateRange();
    const searchInput = document.getElementById('search');
    if (searchInput) {
        searchInput.value = '';
    }

    switch (preset) {
        case 'global-vc':
            // Broad market baseline: all dimensions available.
            break;
        case 'early-stage':
            {
                const selectedRounds = allFilterOptions.round.filter(v => EARLY_STAGE_ROUNDS.includes(v));
                if (selectedRounds.length) {
                    selectedFilters.round = new Set(selectedRounds);
                }
            }
            break;
        case 'hiring-focused':
            selectedFilters.hiring = new Set(['Yes']);
            selectedFilters.linkedin = new Set(
                allFilterOptions.linkedin.includes('Present') ? ['Present'] : allFilterOptions.linkedin
            );
            break;
        default:
            return;
    }

    MULTI_SELECT_KEYS.forEach(syncFilterCheckboxes);
    MULTI_SELECT_KEYS.forEach(updateFilterButtonText);
    filterDeals();
}

function renderDataFreshnessWidget(deals) {
    const latestByCountry = new Map();
    deals.forEach(deal => {
        if (!Number.isFinite(deal.DateValue)) return;
        const nation = cleanString(deal.Nation);
        if (!nation) return;
        const existing = latestByCountry.get(nation);
        if (!existing || deal.DateValue > existing) {
            latestByCountry.set(nation, deal.DateValue);
        }
    });

    const rows = [...latestByCountry.entries()]
        .map(([nation, ts]) => {
            const deltaDays = Math.max(0, Math.floor((getReferenceDate(deals).getTime() - ts) / (24 * 60 * 60 * 1000)));
            const freshness = deltaDays <= 1 ? 'Fresh' : deltaDays <= 3 ? 'Recent' : 'Stale';
            return {
                nation,
                lastSeen: formatDate(ts),
                deltaDays,
                freshness
            };
        })
        .sort((a, b) => a.deltaDays - b.deltaDays)
        .slice(0, 12);

    return `
        <div class="insight-table-wrap" data-insight-table="freshness">
            <table class="simple-table sticky-header-table">
                <thead><tr>
                    ${insightSortableTh('freshness', 'Country', 0)}
                    ${insightSortableTh('freshness', 'Last captured', 1)}
                    ${insightSortableTh('freshness', 'Lag (days)', 2, 'number')}
                    ${insightSortableTh('freshness', 'Status', 3)}
                </tr></thead>
                <tbody>
                    ${rows.map(row => `<tr ${insightRowSortAttrs([row.nation, row.lastSeen, row.deltaDays, row.freshness])}><td>${escapeHtml(row.nation)}</td><td>${escapeHtml(row.lastSeen)}</td><td>${row.deltaDays}</td><td>${escapeHtml(row.freshness)}</td></tr>`).join('')}
                </tbody>
            </table>
        </div>
    `;
}

function severityBadge(level) {
    const mapped = (level || '').toLowerCase();
    const valid = ['high', 'medium', 'low'].includes(mapped) ? mapped : 'low';
    return `<span class="alert-severity alert-${valid}">${valid.toUpperCase()}</span>`;
}

function buildSeverityAlerts(deals, topCountries) {
    const alerts = [];
    const highValue = deals.filter(d => Number.isFinite(d.AmountValue) && d.AmountValue >= 100e6).length;
    const currentOutliers = (Array.isArray(outlierRegistry.records) ? outlierRegistry.records : [])
        .filter(record => record && record.currently_detected);
    const outlierCount = currentOutliers.length;
    const staleCountries = summarizeCountries(deals).filter(c => {
        const lastTs = Math.max(...deals.filter(d => d.Nation === c.name && Number.isFinite(d.DateValue)).map(d => d.DateValue), 0);
        const deltaDays = lastTs ? Math.floor((getReferenceDate(deals).getTime() - lastTs) / (24 * 60 * 60 * 1000)) : 999;
        return deltaDays >= 5;
    }).slice(0, 3);

    if (highValue) {
        alerts.push({
            severity: 'high',
            text: `${highValue} mega deals ($100M+) detected`,
            pageId: 'page-1',
            anchor: 'stats-cards'
        });
    }
    if (outlierCount) {
        alerts.push({
            severity: 'medium',
            text: `${outlierCount} unrealistic amount anomalies need audit`,
            pageId: 'page-2',
            anchor: 'anomaly-audit'
        });
    }
    staleCountries.forEach(country => alerts.push({
        severity: 'low',
        text: `${country.name}: latest deal in this view is older than 5 days`,
        pageId: 'page-2',
        anchor: 'data-freshness'
    }));
    if (!alerts.length && topCountries[0]) {
        alerts.push({
            severity: 'low',
            text: `${topCountries[0].name} leads current volume`,
            pageId: 'page-2',
            anchor: 'country-leaderboard'
        });
    }
    return alerts;
}

function renderSeverityAlerts(alerts) {
    if (!alerts.length) return '<p>No alerts in current filter window.</p>';
    return `
        <ul class="alert-list">
            ${alerts.map(alert => `<li>${severityBadge(alert.severity)} <a href="#${escapeHtml(alert.anchor || '')}" class="alert-link" data-target-page="${escapeHtml(alert.pageId || activePageId)}" data-target-anchor="${escapeHtml(alert.anchor || '')}">${escapeHtml(alert.text)}</a></li>`).join('')}
        </ul>
    `;
}

function renderAnomalyAuditPanel() {
    const records = Array.isArray(outlierRegistry.records) ? outlierRegistry.records : [];
    if (!records.length) {
        return '<p>No currently tracked anomalies in outlier registry.</p>';
    }
    const rows = records
        .slice(0, 20)
        .map(record => {
            const startup = record.startup_name || 'Unknown';
            const nation = record.nation || 'Unknown';
            const amount = record.amount || 'N/A';
            const reason = record.reason || '';
            return `<tr ${insightRowSortAttrs([startup, nation, amount, reason])}><td>${escapeHtml(startup)}</td><td>${escapeHtml(nation)}</td><td>${escapeHtml(amount)}</td><td>${escapeHtml(reason)}</td></tr>`;
        })
        .join('');
    return `
        <div class="insight-table-wrap insight-table-wrap--wide" data-insight-table="anomaly">
            <table class="simple-table sticky-header-table">
                <thead><tr>
                    ${insightSortableTh('anomaly', 'Startup', 0)}
                    ${insightSortableTh('anomaly', 'Nation', 1)}
                    ${insightSortableTh('anomaly', 'Amount', 2)}
                    ${insightSortableTh('anomaly', 'Anomaly reason', 3)}
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
    `;
}

function captureWeekKeyForDedupe(deal) {
    if (Number.isFinite(deal.DateValue)) {
        return String(startOfUtcWeek(new Date(deal.DateValue)).getTime());
    }
    const raw = cleanString(deal.Date_Captured) || cleanString(deal.Date);
    const parsed = raw.includes('T') ? Date.parse(raw) : Date.parse(`${raw.slice(0, 10)}T00:00:00Z`);
    if (Number.isFinite(parsed)) {
        return String(startOfUtcWeek(new Date(parsed)).getTime());
    }
    return `s:${raw.slice(0, 10)}`;
}

function buildDealDedupFingerprint(deal) {
    const orgCountry = cleanString(deal.Country).toLowerCase() || cleanString(deal.Nation).toLowerCase();
    const startup = cleanString(deal.Startup_Name).toLowerCase();
    const amountKey = Number.isFinite(deal.AmountValue)
        ? `v:${deal.AmountValue}`
        : `a:${cleanString(deal.Amount).toLowerCase()}`;
    const weekKey = captureWeekKeyForDedupe(deal);
    return `${orgCountry}|${startup}|${amountKey}|${weekKey}`;
}

function isNationMatchingOrgCountry(deal) {
    const country = cleanString(deal.Country);
    const nation = cleanString(deal.Nation);
    return Boolean(country && nation && country === nation);
}

function pickPreferredDuplicateDeal(existing, incoming) {
    const orgMatchA = isNationMatchingOrgCountry(existing);
    const orgMatchB = isNationMatchingOrgCountry(incoming);
    if (orgMatchB && !orgMatchA) return incoming;
    if (orgMatchA && !orgMatchB) return existing;
    const tA = Number.isFinite(existing.DateValue) ? existing.DateValue : 0;
    const tB = Number.isFinite(incoming.DateValue) ? incoming.DateValue : 0;
    return tB >= tA ? incoming : existing;
}

function dedupeDealsByFingerprint(deals) {
    const byFingerprint = new Map();
    deals.forEach(deal => {
        const fp = buildDealDedupFingerprint(deal);
        if (!byFingerprint.has(fp)) {
            byFingerprint.set(fp, deal);
            return;
        }
        byFingerprint.set(fp, pickPreferredDuplicateDeal(byFingerprint.get(fp), deal));
    });
    return [...byFingerprint.values()];
}

function normalizeDeal(deal, fallbackNation) {
    const nation = cleanString(deal.Nation) || cleanString(deal.Country) || fallbackNation;
    const country = cleanString(deal.Country) || nation;
    const capturedDate = cleanString(deal.Date_Captured) || cleanString(deal.Date);
    const dateValue = parseDateValue(capturedDate);
    const amountInfo = parseAmountInfo(deal.Amount);

    return {
        ...deal,
        Country: country,
        Nation: nation,
        Flag: cleanString(deal.Flag) || getCountryFlag(nation),
        Date: capturedDate,
        Date_Captured: capturedDate,
        DateValue: dateValue,
        AmountValue: amountInfo.usdValue,
        AmountCurrency: amountInfo.currency,
        AmountOriginalValue: amountInfo.originalValue,
        AmountWasConverted: amountInfo.isConverted,
        ExcludedReason: getDealExclusionReason({
            Startup_Name: deal.Startup_Name,
            Nation: nation,
            DateValue: dateValue,
            AmountValue: amountInfo.usdValue
        }),
        RoundFilter: normalizeCategoryValue(deal.Round),
        TierFilter: normalizeTierValue(deal.Tier),
        LinkedInFilter: hasUsefulLinks(deal.LinkedIn_Profile) ? 'Present' : 'Missing',
        HiringFilter: normalizeHiringValue(deal.Hiring),
        CareersFilter: hasUsefulLinks(deal.Careers_Link) ? 'Present' : 'Missing'
    };
}

function initializeFilterState() {
    nations = sortAlphabetically(uniqueValues(allDeals.map(deal => deal.Nation)));

    allFilterOptions = createEmptyFilterOptions();
    allFilterOptions.nation = nations;
    allFilterOptions.round = sortWithUnknownLast(uniqueValues(allDeals.map(deal => deal.RoundFilter)));
    allFilterOptions.tier = sortTierValues(uniqueValues(allDeals.map(deal => deal.TierFilter)));
    allFilterOptions.linkedin = sortByPreferredOrder(uniqueValues(allDeals.map(deal => deal.LinkedInFilter)), ['Present', 'Missing']);
    allFilterOptions.hiring = sortByPreferredOrder(uniqueValues(allDeals.map(deal => deal.HiringFilter)), ['Yes', 'No', UNKNOWN_LABEL]);
    allFilterOptions.careers = sortByPreferredOrder(uniqueValues(allDeals.map(deal => deal.CareersFilter)), ['Present', 'Missing']);

    filterOptions = createEmptyFilterOptions();
    Object.keys(allFilterOptions).forEach(key => {
        filterOptions[key] = [...allFilterOptions[key]];
    });

    selectedFilters = createEmptySelectedFilters();
    Object.keys(filterOptions).forEach(key => {
        selectedFilters[key] = new Set(filterOptions[key]);
    });

    initializeAmountRange();
}

function setupEventListeners() {
    setupPageNavigation();
    Object.entries(MULTI_SELECT_FILTERS).forEach(([key, config]) => {
        const button = document.getElementById(config.buttonId);
        const selectAllButton = document.getElementById(config.selectAllId);
        const clearAllButton = document.getElementById(config.clearAllId);

        if (button) {
            button.addEventListener('click', event => toggleDropdown(key, event));
        }

        if (selectAllButton) {
            selectAllButton.addEventListener('click', event => selectAllOptions(key, event));
        }

        if (clearAllButton) {
            clearAllButton.addEventListener('click', event => clearAllOptions(key, event));
        }
    });

    const dateFrom = document.getElementById('date-from');
    const dateTo = document.getElementById('date-to');
    const search = document.getElementById('search');
    const resetBtn = document.getElementById('reset-filters');
    const exportBtn = document.getElementById('export-csv');
    const amountMin = document.getElementById('amount-min');
    const amountMax = document.getElementById('amount-max');
    const amountMinInput = document.getElementById('amount-min-input');
    const amountMaxInput = document.getElementById('amount-max-input');
    const presetButtons = Array.from(document.querySelectorAll('.btn-preset'));
    const globalCountrySelector = document.getElementById('country-intel-select-global');

    if (dateFrom) dateFrom.addEventListener('change', filterDeals);
    if (dateTo) dateTo.addEventListener('change', filterDeals);
    if (search) search.addEventListener('input', filterDeals);
    if (resetBtn) resetBtn.addEventListener('click', resetFilters);
    if (exportBtn) exportBtn.addEventListener('click', downloadFilteredCsv);
    if (amountMin) amountMin.addEventListener('input', () => handleAmountRangeInput('min'));
    if (amountMax) amountMax.addEventListener('input', () => handleAmountRangeInput('max'));
    if (amountMinInput) {
        amountMinInput.addEventListener('change', () => handleAmountTextInput('min'));
        amountMinInput.addEventListener('blur', () => handleAmountTextInput('min'));
    }
    if (amountMaxInput) {
        amountMaxInput.addEventListener('change', () => handleAmountTextInput('max'));
        amountMaxInput.addEventListener('blur', () => handleAmountTextInput('max'));
    }
    presetButtons.forEach(button => {
        button.addEventListener('click', () => applyFilterPreset(button.dataset.preset || ''));
        button.setAttribute('aria-pressed', 'false');
        button.setAttribute('type', 'button');
    });
    if (globalCountrySelector) {
        globalCountrySelector.addEventListener('change', event => {
            selectedCountryInsight = event.target.value;
            updateCountryContextSubtitle(selectedCountryInsight);
            renderCountryIntelligencePage(filteredDeals);
        });
    }

    document.addEventListener('click', event => {
        if (!event.target.closest('.multi-select-wrapper')) {
            closeAllDropdowns();
        }
    });

    document.addEventListener('keydown', event => {
        if (event.key === 'Escape') {
            closeAllDropdowns();
        }
    });
}

function populateAllFilters() {
    Object.keys(MULTI_SELECT_FILTERS).forEach(populateFilter);
}

function populateFilter(key) {
    const config = MULTI_SELECT_FILTERS[key];
    const container = document.getElementById(config.containerId);

    if (!container) {
        return;
    }

    const availableSet = new Set(filterOptions[key]);
    container.innerHTML = '';

    allFilterOptions[key].forEach(option => {
        const isAvailable = availableSet.has(option);
        const label = document.createElement('label');
        label.className = isAvailable ? 'checkbox-item' : 'checkbox-item checkbox-item-unavailable';

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = option;
        checkbox.checked = selectedFilters[key].has(option);
        checkbox.addEventListener('change', event => handleFilterCheckboxChange(key, event));

        const text = document.createElement('span');
        text.textContent = config.formatOption ? config.formatOption(option) : option;

        label.appendChild(checkbox);
        label.appendChild(text);
        container.appendChild(label);
    });

    updateFilterButtonText(key);
}

function handleFilterCheckboxChange(key, event) {
    const value = event.target.value;

    if (event.target.checked) {
        selectedFilters[key].add(value);
    } else {
        selectedFilters[key].delete(value);
    }

    updateFilterButtonText(key);
    filterDeals();
}

function toggleDropdown(key, event) {
    event.stopPropagation();

    const config = MULTI_SELECT_FILTERS[key];
    const dropdown = document.getElementById(config.dropdownId);

    if (!dropdown) {
        return;
    }

    const isHidden = dropdown.classList.contains('hidden');
    closeAllDropdowns();

    if (isHidden) {
        dropdown.classList.remove('hidden');
    }
}

function closeAllDropdowns() {
    Object.values(MULTI_SELECT_FILTERS).forEach(config => {
        const dropdown = document.getElementById(config.dropdownId);
        if (dropdown) {
            dropdown.classList.add('hidden');
        }
    });
}

function selectAllOptions(key, event) {
    event.stopPropagation();
    selectedFilters[key] = new Set(allFilterOptions[key]);
    syncFilterCheckboxes(key);
    updateFilterButtonText(key);
    filterDeals();
}

function clearAllOptions(key, event) {
    event.stopPropagation();
    selectedFilters[key].clear();
    syncFilterCheckboxes(key);
    updateFilterButtonText(key);
    filterDeals();
}

function syncFilterCheckboxes(key) {
    const config = MULTI_SELECT_FILTERS[key];
    const container = document.getElementById(config.containerId);

    if (!container) {
        return;
    }

    container.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
        checkbox.checked = selectedFilters[key].has(checkbox.value);
    });
}

function updateFilterButtonText(key) {
    const config = MULTI_SELECT_FILTERS[key];
    const button = document.getElementById(config.buttonId);

    if (!button) {
        return;
    }

    const masterTotal = allFilterOptions[key].length;
    const count = selectedFilters[key].size;

    if (count === 0) {
        button.innerHTML = `${config.emptyLabel} <span class="arrow">▼</span>`;
    } else if (count === masterTotal) {
        button.innerHTML = `All ${config.pluralLabel} (${masterTotal}) <span class="arrow">▼</span>`;
    } else {
        button.innerHTML = `${count} of ${masterTotal} ${config.pluralLabel} <span class="arrow">▼</span>`;
    }
}

function initializeAmountRange() {
    const amountValues = allDeals
        .map(deal => deal.AmountValue)
        .filter(value => Number.isFinite(value));

    if (amountValues.length === 0) {
        amountRange = {
            availableMin: 0,
            availableMax: 0,
            selectedMin: 0,
            selectedMax: 0,
            hasValues: false
        };
        return;
    }

    amountRange = {
        availableMin: Math.min(...amountValues),
        availableMax: Math.max(...amountValues),
        selectedMin: Math.min(...amountValues),
        selectedMax: Math.max(...amountValues),
        hasValues: true
    };
}

function initializeAmountRangeControls() {
    const amountMin = document.getElementById('amount-min');
    const amountMax = document.getElementById('amount-max');
    const amountMinInput = document.getElementById('amount-min-input');
    const amountMaxInput = document.getElementById('amount-max-input');
    const summary = document.getElementById('amount-range-summary');

    if (!amountMin || !amountMax || !amountMinInput || !amountMaxInput || !summary) {
        return;
    }

    if (!amountRange.hasValues) {
        [amountMin, amountMax, amountMinInput, amountMaxInput].forEach(element => {
            element.disabled = true;
            element.value = '';
        });
        summary.textContent = 'No amount data available';
        updateAmountRangeUI();
        return;
    }

    [amountMin, amountMax].forEach(input => {
        input.disabled = false;
        input.min = '0';
        input.max = String(AMOUNT_SLIDER_MAX);
        input.step = '1';
    });

    amountMinInput.disabled = false;
    amountMaxInput.disabled = false;
    updateAmountRangeUI();
}

function handleAmountRangeInput(changedSide) {
    const amountMin = document.getElementById('amount-min');
    const amountMax = document.getElementById('amount-max');

    if (!amountMin || !amountMax || !amountRange.hasValues) {
        return;
    }

    let minSliderValue = Number(amountMin.value);
    let maxSliderValue = Number(amountMax.value);

    if (changedSide === 'min' && minSliderValue > maxSliderValue) {
        maxSliderValue = minSliderValue;
    }

    if (changedSide === 'max' && maxSliderValue < minSliderValue) {
        minSliderValue = maxSliderValue;
    }

    amountRange.selectedMin = sliderValueToAmount(minSliderValue);
    amountRange.selectedMax = sliderValueToAmount(maxSliderValue);
    updateAmountRangeUI();
    filterDeals();
}

function handleAmountTextInput(changedSide) {
    const input = document.getElementById(changedSide === 'min' ? 'amount-min-input' : 'amount-max-input');

    if (!input || !amountRange.hasValues) {
        return;
    }

    const raw = cleanString(input.value);
    const defaultValue = changedSide === 'min' ? amountRange.availableMin : amountRange.availableMax;
    const parsedValue = raw ? parseAmount(raw) : defaultValue;

    if (!Number.isFinite(parsedValue)) {
        updateAmountRangeUI();
        return;
    }

    const nextValue = clamp(parsedValue, amountRange.availableMin, amountRange.availableMax);

    if (changedSide === 'min') {
        amountRange.selectedMin = Math.min(nextValue, amountRange.selectedMax);
    } else {
        amountRange.selectedMax = Math.max(nextValue, amountRange.selectedMin);
    }

    updateAmountRangeUI();
    filterDeals();
}

function updateAmountRangeUI() {
    const amountMin = document.getElementById('amount-min');
    const amountMax = document.getElementById('amount-max');
    const amountMinInput = document.getElementById('amount-min-input');
    const amountMaxInput = document.getElementById('amount-max-input');
    const summary = document.getElementById('amount-range-summary');
    const rangeFill = document.getElementById('amount-range-fill');

    if (!amountMin || !amountMax || !amountMinInput || !amountMaxInput || !summary || !rangeFill) {
        return;
    }

    if (!amountRange.hasValues) {
        amountMin.value = '0';
        amountMax.value = '0';
        amountMinInput.value = '';
        amountMaxInput.value = '';
        summary.textContent = 'No amount data available';
        rangeFill.style.left = '0%';
        rangeFill.style.width = '0%';
        return;
    }

    const minSliderValue = amountToSliderValue(amountRange.selectedMin);
    const maxSliderValue = amountToSliderValue(amountRange.selectedMax);

    amountMin.value = String(minSliderValue);
    amountMax.value = String(maxSliderValue);
    amountMinInput.value = formatAmountInputValue(amountRange.selectedMin);
    amountMaxInput.value = formatAmountInputValue(amountRange.selectedMax);

    const leftPercent = (minSliderValue / AMOUNT_SLIDER_MAX) * 100;
    const rightPercent = (maxSliderValue / AMOUNT_SLIDER_MAX) * 100;

    rangeFill.style.left = `${leftPercent}%`;
    rangeFill.style.width = `${Math.max(rightPercent - leftPercent, 0)}%`;

    const isFullRange =
        amountRange.selectedMin === amountRange.availableMin &&
        amountRange.selectedMax === amountRange.availableMax;

    summary.textContent = isFullRange
        ? `Any amount (${formatCurrencyCompact(amountRange.availableMin)} - ${formatCurrencyCompact(amountRange.availableMax)})`
        : `${formatCurrencyCompact(amountRange.selectedMin)} - ${formatCurrencyCompact(amountRange.selectedMax)}`;
}

function setDefaultDateRange() {
    const dateFrom = document.getElementById('date-from');
    const dateTo = document.getElementById('date-to');

    if (!dateFrom || !dateTo) {
        return;
    }

    const availableDates = allDeals
        .map(deal => deal.DateValue)
        .filter(value => Number.isFinite(value));

    if (availableDates.length === 0) {
        dateFrom.value = '';
        dateTo.value = '';
        return;
    }

    const maxDate = new Date(Math.max(...availableDates));
    const ninetyDaysAgo = new Date(maxDate.getTime() - (90 * 24 * 60 * 60 * 1000));

    dateTo.value = formatDateInputValue(maxDate);
    dateFrom.value = formatDateInputValue(ninetyDaysAgo);
}

function getDateFilterLabel() {
    const dateFromInput = document.getElementById('date-from');
    const dateToInput = document.getElementById('date-to');
    const from = cleanString(dateFromInput ? dateFromInput.value : '');
    const to = cleanString(dateToInput ? dateToInput.value : '');
    if (!from && !to) {
        return 'All dates';
    }
    if (from && to) {
        return `${from} to ${to}`;
    }
    if (from) {
        return `From ${from}`;
    }
    return `Until ${to}`;
}

function updateCountryContextSubtitle(country) {
    const node = document.getElementById('country-context-subtitle');
    if (!node) return;
    node.textContent = `Country intelligence currently focused on: ${country || 'All countries'}`;
}

function syncGlobalCountrySelector(countries) {
    const group = document.getElementById('country-intel-filter-group');
    const selector = document.getElementById('country-intel-select-global');
    if (!group || !selector) {
        return;
    }

    const show = activePageId === 'page-3' && Array.isArray(countries) && countries.length > 0;
    group.classList.toggle('hidden', !show);
    if (!show) {
        return;
    }

    const options = countries
        .map(country => `<option value="${escapeHtml(country.name)}" ${country.name === selectedCountryInsight ? 'selected' : ''}>${escapeHtml(country.name)}</option>`)
        .join('');
    selector.innerHTML = options;
}

function filterDeals() {
    let filtered = [...allDeals];

    filtered = applyMultiSelectFilter(filtered, 'nation');
    filtered = applyMultiSelectFilter(filtered, 'round');
    filtered = applyMultiSelectFilter(filtered, 'tier');
    filtered = applyMultiSelectFilter(filtered, 'linkedin');
    filtered = applyMultiSelectFilter(filtered, 'hiring');
    filtered = applyMultiSelectFilter(filtered, 'careers');

    filtered = applyAmountFilter(filtered);
    filtered = applyDateFilter(filtered);
    filtered = applySearchFilter(filtered);

    filteredDeals = filtered;
    updateAvailableFilterOptions();
    syncSavedViewButtons();
    applyCurrentSort();
    displayStats(filteredDeals);
    renderEnhancedPages(filteredDeals);

    const refDate = getReferenceDate(filteredDeals);
    const tableDeals = getRecentDeals(filteredDeals, refDate, 7);
    displayDeals(tableDeals, refDate);
}

function getRecentDeals(deals, refDate, days) {
    const endTime = endOfUtcDay(refDate).getTime();
    const startTime = startOfUtcDay(new Date(refDate.getTime() - ((days - 1) * 24 * 60 * 60 * 1000))).getTime();
    return deals.filter(deal =>
        Number.isFinite(deal.DateValue) &&
        deal.DateValue >= startTime &&
        deal.DateValue <= endTime
    );
}

function applyAllFiltersExcept(excludeKey) {
    let filtered = [...allDeals];
    MULTI_SELECT_KEYS.forEach(key => {
        if (key !== excludeKey) {
            const selected = selectedFilters[key];
            const allOptions = allFilterOptions[key];
            const field = MULTI_SELECT_FILTERS[key].field;
            if (!allOptions.length || selected.size === 0) {
                filtered = [];
            } else if (selected.size < allOptions.length) {
                filtered = filtered.filter(deal => selected.has(deal[field]));
            }
        }
    });
    filtered = applyAmountFilter(filtered);
    filtered = applyDateFilter(filtered);
    filtered = applySearchFilter(filtered);
    return filtered;
}

function updateAvailableFilterOptions() {
    const computed = {};
    MULTI_SELECT_KEYS.forEach(key => {
        const config = MULTI_SELECT_FILTERS[key];
        const field = config.field;
        const dealsForThisFilter = applyAllFiltersExcept(key);
        const availableValues = new Set(dealsForThisFilter.map(d => d[field]).filter(Boolean));
        const sortFn = getSortFnForKey(key);
        const masterOptions = allFilterOptions[key];
        computed[key] = sortFn(masterOptions.filter(opt => availableValues.has(opt)));
    });

    MULTI_SELECT_KEYS.forEach(key => {
        const newOptions = computed[key];
        const oldOptions = filterOptions[key];
        filterOptions[key] = newOptions;

        const optionsChanged = newOptions.length !== oldOptions.length ||
            newOptions.some((v, i) => v !== oldOptions[i]);

        if (optionsChanged) {
            populateFilter(key);
        } else {
            updateFilterButtonText(key);
        }
    });
}

function getSortFnForKey(key) {
    switch (key) {
        case 'nation': return sortAlphabetically;
        case 'round': return sortWithUnknownLast;
        case 'tier': return sortTierValues;
        case 'linkedin': return v => sortByPreferredOrder(v, ['Present', 'Missing']);
        case 'hiring': return v => sortByPreferredOrder(v, ['Yes', 'No', UNKNOWN_LABEL]);
        case 'careers': return v => sortByPreferredOrder(v, ['Present', 'Missing']);
        default: return sortAlphabetically;
    }
}

function applyMultiSelectFilter(deals, key) {
    const allOptions = allFilterOptions[key];
    const selected = selectedFilters[key];
    const field = MULTI_SELECT_FILTERS[key].field;

    if (!allOptions.length) {
        return deals;
    }

    if (selected.size === 0) {
        return [];
    }

    if (selected.size === allOptions.length) {
        return deals;
    }

    return deals.filter(deal => selected.has(deal[field]));
}

function applyAmountFilter(deals) {
    if (!amountRange.hasValues) {
        return deals;
    }

    const isFullRange =
        amountRange.selectedMin === amountRange.availableMin &&
        amountRange.selectedMax === amountRange.availableMax;

    if (isFullRange) {
        return deals;
    }

    return deals.filter(deal =>
        Number.isFinite(deal.AmountValue) &&
        deal.AmountValue >= amountRange.selectedMin &&
        deal.AmountValue <= amountRange.selectedMax
    );
}

function applyDateFilter(deals) {
    const dateFromInput = document.getElementById('date-from');
    const dateToInput = document.getElementById('date-to');
    const dateFrom = dateFromInput ? dateFromInput.value : '';
    const dateTo = dateToInput ? dateToInput.value : '';

    if (!dateFrom && !dateTo) {
        return deals;
    }

    const fromTime = dateFrom ? Date.parse(`${dateFrom}T00:00:00Z`) : null;
    const toTime = dateTo ? Date.parse(`${dateTo}T23:59:59Z`) : null;

    return deals.filter(deal => {
        if (!Number.isFinite(deal.DateValue)) {
            return true;
        }

        if (Number.isFinite(fromTime) && deal.DateValue < fromTime) {
            return false;
        }

        if (Number.isFinite(toTime) && deal.DateValue > toTime) {
            return false;
        }

        return true;
    });
}

function applySearchFilter(deals) {
    const searchInput = document.getElementById('search');
    const searchTerm = searchInput ? searchInput.value.toLowerCase().trim() : '';

    if (!searchTerm) {
        return deals;
    }

    return deals.filter(deal =>
        SEARCH_FIELDS.some(field =>
            cleanString(deal[field]).toLowerCase().includes(searchTerm)
        )
    );
}

function resetFilters() {
    Object.keys(allFilterOptions).forEach(key => {
        filterOptions[key] = [...allFilterOptions[key]];
        selectedFilters[key] = new Set(filterOptions[key]);
        syncFilterCheckboxes(key);
        updateFilterButtonText(key);
    });

    initializeAmountRange();
    initializeAmountRangeControls();
    setDefaultDateRange();

    const searchInput = document.getElementById('search');
    if (searchInput) {
        searchInput.value = '';
    }

    currentSort = { column: null, direction: 'asc' };
    filterDeals();
}

function displayStats(deals) {
    const statsContainer = document.getElementById('stats-cards');

    if (!statsContainer) {
        return;
    }

    const overviewMetrics = calculateMetrics(deals);
    const referenceDate = getReferenceDate(deals);
    const periodCards = PERIOD_DEFINITIONS.map(definition =>
        buildPeriodCard(definition, deals, referenceDate)
    ).join('');

    statsContainer.innerHTML = `
        <section class="stats-shell">
            <div class="stats-overview-card">
                <div class="stats-overview-header">
                    <div>
                        <p class="section-kicker">Filtered View</p>
                        <h2>${overviewMetrics.totalDeals.toLocaleString()} deals matching the current dashboard filters</h2>
                        <p class="stats-caption">
                            Time-window snapshots below are aligned to the latest visible capture date:
                            <strong>${formatDate(referenceDate)}</strong>
                        </p>
                    </div>
                    <div class="stats-overview-pills">
                        <span class="meta-pill">Median deal ${formatCurrencyCompact(overviewMetrics.medianDealSize)}</span>
                        <span class="meta-pill">Largest deal ${formatCurrencyCompact(overviewMetrics.largestDealAmount)}</span>
                        <span class="meta-pill">${overviewMetrics.linkLabel}</span>
                    </div>
                </div>

                <div class="overview-metric-grid">
                    ${buildOverviewMetric('💼', 'Total Deals', overviewMetrics.totalDeals.toLocaleString())}
                    ${buildOverviewMetric('💰', 'Total Funding', formatCurrencyCompact(overviewMetrics.totalFunding))}
                    ${buildOverviewMetric('🌍', 'Countries', overviewMetrics.uniqueCountries.toLocaleString())}
                    ${buildOverviewMetric('📊', 'Avg Deal Size', formatCurrencyCompact(overviewMetrics.avgDealSize))}
                </div>

                <div class="stats-overview-meta">
                    <span class="meta-pill">Amounts known: ${overviewMetrics.amountCoverageLabel}</span>
                    <span class="meta-pill">${overviewMetrics.hiringLabel}</span>
                    <span class="meta-pill">Top round: ${escapeHtml(overviewMetrics.topRound)}</span>
                    <span class="meta-pill">Founders listed: ${overviewMetrics.founderCoverageLabel}</span>
                </div>
            </div>

            <div class="highlight-cards-grid">
                ${buildHighlightCard('🏆', 'Top Round', escapeHtml(overviewMetrics.topRound), `${overviewMetrics.topRoundCount} deals`)}
                ${buildHighlightCard('🚀', 'Largest Deal', formatCurrencyCompact(overviewMetrics.largestDealAmount), escapeHtml(overviewMetrics.largestDealName))}
                ${buildHighlightCard('💼', 'Actively Hiring', overviewMetrics.hiringCount.toLocaleString(), overviewMetrics.hiringLabel)}
                ${buildHighlightCard('⭐', 'Top Tier', escapeHtml(overviewMetrics.topTier), `${overviewMetrics.topTierCount} deals`)}
            </div>

            <div class="period-section-header">
                <div>
                    <p class="section-kicker">Time Windows</p>
                    <h3>Current day, week, and month against prior periods and all time</h3>
                </div>
            </div>

            <div class="period-grid">
                ${periodCards}
            </div>
        </section>
    `;
}

function buildHighlightCard(icon, label, value, subtitle) {
    return `
        <div class="highlight-card">
            <span class="highlight-icon">${icon}</span>
            <span class="highlight-value">${value}</span>
            <span class="highlight-label">${label}</span>
            ${subtitle ? `<span class="highlight-sub">${subtitle}</span>` : ''}
        </div>
    `;
}

function buildOverviewMetric(icon, label, value) {
    return `
        <div class="overview-metric-tile">
            <span class="metric-icon">${icon}</span>
            <span class="metric-value">${value}</span>
            <span class="metric-label">${label}</span>
        </div>
    `;
}

function buildPeriodCard(definition, deals, referenceDate) {
    const range = definition.getRange(referenceDate, deals);
    const dealsInRange = deals.filter(deal =>
        Number.isFinite(deal.DateValue) &&
        deal.DateValue >= range.start.getTime() &&
        deal.DateValue <= range.end.getTime()
    );
    const metrics = calculateMetrics(dealsInRange);

    return `
        <article class="period-card">
            <div class="period-card-header">
                <div>
                    <p class="section-kicker">${definition.icon}</p>
                    <h4>${definition.label}</h4>
                </div>
                <span class="period-range">${formatDateRange(range.start, range.end)}</span>
            </div>

            <div class="period-stat-grid period-stat-grid--3col">
                <div class="period-stat">
                    <span>Deals</span>
                    <strong>${metrics.totalDeals.toLocaleString()}</strong>
                </div>
                <div class="period-stat">
                    <span>Funding</span>
                    <strong>${formatCurrencyCompact(metrics.totalFunding)}</strong>
                </div>
                <div class="period-stat">
                    <span>Countries</span>
                    <strong>${metrics.uniqueCountries.toLocaleString()}</strong>
                </div>
            </div>
            <div class="period-stat-grid period-stat-grid--2col-centered">
                <div class="period-stat period-stat--compact-metric">
                    <span class="period-stat-label period-stat-label--single-line">Avg Size</span>
                    <strong>${formatCurrencyCompact(metrics.avgDealSize)}</strong>
                </div>
                <div class="period-stat period-stat--compact-metric">
                    <span class="period-stat-label period-stat-label--single-line">Median Size</span>
                    <strong>${formatCurrencyCompact(metrics.medianDealSize)}</strong>
                </div>
            </div>

            <div class="period-card-footer">
                <span class="period-pill">Top round: ${escapeHtml(metrics.topRound)}</span>
                <span class="period-pill">${metrics.hiringLabel}</span>
                <span class="period-pill">${metrics.linkLabel}</span>
                <span class="period-pill">${metrics.amountKnownLabel}</span>
            </div>
        </article>
    `;
}

function calculateMetrics(deals) {
    const total = deals.length;
    const dealsWithAmounts = deals.filter(deal => Number.isFinite(deal.AmountValue));
    const convertedAmountCount = dealsWithAmounts.filter(deal => deal.AmountWasConverted).length;
    const totalFunding = dealsWithAmounts.reduce((sum, deal) => sum + deal.AmountValue, 0);
    const uniqueCountries = new Set(deals.map(deal => deal.Nation).filter(Boolean)).size;
    const avgDealSize = dealsWithAmounts.length > 0 ? totalFunding / dealsWithAmounts.length : 0;
    const medianDealSize = getMedian(dealsWithAmounts.map(deal => deal.AmountValue));
    const largestDealAmount = dealsWithAmounts.length > 0
        ? Math.max(...dealsWithAmounts.map(deal => deal.AmountValue))
        : 0;

    const hiringCount = deals.filter(d => d.HiringFilter === 'Yes').length;
    const notHiringCount = deals.filter(d => d.HiringFilter === 'No').length;
    const hiringUnknownCount = total - hiringCount - notHiringCount;

    const linkedinPresent = deals.filter(d => d.LinkedInFilter === 'Present').length;
    const careersPresent = deals.filter(d => d.CareersFilter === 'Present').length;
    const founderCount = deals.filter(deal => hasMeaningfulValue(deal.Founders)).length;

    const topRound = getTopCategory(deals.map(deal => deal.RoundFilter));
    const topRoundCount = deals.filter(deal => deal.RoundFilter === topRound).length;

    const largestDealObj = dealsWithAmounts.length > 0
        ? dealsWithAmounts.reduce((max, deal) => deal.AmountValue > max.AmountValue ? deal : max, dealsWithAmounts[0])
        : null;
    const largestDealName = largestDealObj ? (largestDealObj.Startup_Name || 'Unknown') : 'N/A';

    const topTier = getTopCategory(deals.map(deal => deal.TierFilter));
    const topTierCount = deals.filter(deal => deal.TierFilter === topTier).length;

    const hiringKnown = hiringCount + notHiringCount;
    let hiringLabel;
    if (!total) {
        hiringLabel = 'No hiring signal';
    } else if (!hiringKnown) {
        hiringLabel = 'No hiring signal';
    } else {
        const pctHiring = Math.round((hiringCount / total) * 100);
        const pctKnown = Math.round((hiringKnown / total) * 100);
        hiringLabel = `${pctHiring}% known hiring`;
    }

    const linkCount = linkedinPresent + careersPresent;
    let linkLabel;
    if (!total) {
        linkLabel = 'No links present';
    } else if (!linkCount) {
        linkLabel = 'No links present';
    } else {
        const pctLinks = Math.round((linkCount / total) * 100);
        linkLabel = `${pctLinks}% known link coverage`;
    }

    const amountKnownPct = total ? Math.round((dealsWithAmounts.length / total) * 100) : 0;
    const amountKnownLabel = total ? `${amountKnownPct}% amounts known` : 'No amount data';

    return {
        totalDeals: total,
        totalFunding,
        uniqueCountries,
        avgDealSize,
        medianDealSize,
        largestDealAmount,
        largestDealName,
        topRound,
        topRoundCount,
        hiringCount,
        notHiringCount,
        hiringUnknownCount,
        topTier,
        topTierCount,
        linkedinPresent,
        careersPresent,
        amountCoverageLabel: convertedAmountCount
            ? `${dealsWithAmounts.length}/${total || 0} with USD-equivalent amounts (${convertedAmountCount} converted)`
            : `${dealsWithAmounts.length}/${total || 0} with disclosed amounts`,
        amountKnownLabel,
        hiringLabel,
        linkLabel,
        founderCoverageLabel: total ? `${founderCount}/${total} with founder names` : 'No founder data'
    };
}

function getReferenceDate(deals) {
    const candidates = (deals.length ? deals : allDeals)
        .map(deal => deal.DateValue)
        .filter(value => Number.isFinite(value));

    if (!candidates.length) {
        return new Date();
    }

    return new Date(Math.max(...candidates));
}

function displayDeals(deals, refDate) {
    const dealCount = document.getElementById('deal-count');
    const dealsTable = document.getElementById('deals-table');
    const dealsSubtitle = document.getElementById('deals-subtitle');

    if (dealCount) {
        dealCount.textContent = deals.length.toLocaleString();
    }

    if (dealsSubtitle && refDate) {
        const sevenDaysAgo = new Date(refDate.getTime() - (6 * 24 * 60 * 60 * 1000));
        dealsSubtitle.textContent = `Showing deals from ${formatDate(sevenDaysAgo)} to ${formatDate(refDate)}. Cards above reflect all ${filteredDeals.length.toLocaleString()} filtered deals.`;
    }

    if (!dealsTable) {
        return;
    }

    if (deals.length === 0) {
        dealsTable.innerHTML = `<p class="no-results">No deals in the last 7 days matching your filters.</p>`;
        return;
    }

    const headerRow = TABLE_COLUMNS.map(column => `
        <th onclick="sortDeals('${column.key}')" class="sortable ${escapeHtml(column.className || '')}">
            ${escapeHtml(column.label)} <span class="sort-icon">${getSortIcon(column.key)}</span>
        </th>
    `).join('');

    const bodyRows = deals.map(deal => `
        <tr>
            ${TABLE_COLUMNS.map(column => renderTableCell(deal, column)).join('')}
        </tr>
    `).join('');

    dealsTable.innerHTML = `
        <div class="table-scroll-wrapper">
            <table>
                <thead>
                    <tr>${headerRow}</tr>
                </thead>
                <tbody>${bodyRows}</tbody>
            </table>
        </div>
    `;
}

function renderTableCell(deal, column) {
    const className = column.className ? ` ${column.className}` : '';

    switch (column.key) {
        case 'Nation':
            return `<td class="nation-cell${className}"><span class="nation-flag">${escapeHtml(deal.Flag || '🌍')}</span> ${escapeHtml(deal.Nation || deal.Country || 'N/A')}</td>`;
        case 'Startup_Name':
            return `<td class="startup-cell${className}"><strong class="startup-name">${linkifyText(deal.Startup_Name || 'N/A')}</strong></td>`;
        case 'Amount':
            return `<td class="amount-cell${className}"><strong>${escapeHtml(formatDealAmountForTable(deal))}</strong></td>`;
        case 'Hiring':
            return `<td class="${className.trim()}">${renderHiringCell(deal)}</td>`;
        case 'Tier':
            return `<td class="${className.trim()}">${renderBadge(deal.Tier || UNKNOWN_LABEL, 'tier')}</td>`;
        case 'LinkedIn_Profile':
            return `<td class="link-column${className}">${renderViewLink(deal.LinkedIn_Profile)}</td>`;
        case 'Careers_Link':
            return `<td class="link-column${className}">${renderViewLink(deal.Careers_Link)}</td>`;
        case 'Date_Captured':
            return `<td class="${className.trim()}">${escapeHtml(formatDate(deal.Date_Captured))}</td>`;
        default:
            return `<td class="${className.trim()}">${renderTextValue(deal[column.key])}</td>`;
    }
}

function renderViewLink(value) {
    const links = extractUrls(value);
    if (!links.length) {
        return '<span class="cell-muted">N/A</span>';
    }
    return links.map(url =>
        `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer" class="view-link">View ↗</a>`
    ).join(' ');
}

function renderHiringCell(deal) {
    const filter = deal.HiringFilter;
    if (filter === 'Yes') {
        return renderBadge('Yes', 'positive');
    }
    if (filter === 'No') {
        return renderBadge('No', 'negative');
    }
    return '<span class="cell-muted">Unknown</span>';
}

function renderTextValue(value) {
    if (!hasMeaningfulValue(value)) {
        return '<span class="cell-muted">N/A</span>';
    }

    return linkifyText(value);
}

function renderLinkField(value, fallbackLabel) {
    const links = extractUrls(value);

    if (!links.length) {
        return renderTextValue(value);
    }

    return `
        <div class="link-stack">
            ${links.map((url, index) => `
                <a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer" class="text-link">
                    ${escapeHtml(getLinkLabel(url, fallbackLabel, index))}
                </a>
            `).join('')}
        </div>
    `;
}

function renderBadge(value, variant) {
    return `<span class="status-badge status-${escapeHtml(variant)}">${escapeHtml(cleanString(value) || UNKNOWN_LABEL)}</span>`;
}

function sortDeals(column, toggle = true) {
    if (toggle) {
        if (currentSort.column === column) {
            currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
        } else {
            currentSort.column = column;
            currentSort.direction = 'asc';
        }
    }

    applyCurrentSort();
    displayStats(filteredDeals);
    renderEnhancedPages(filteredDeals);

    const refDate = getReferenceDate(filteredDeals);
    const tableDeals = getRecentDeals(filteredDeals, refDate, 7);
    displayDeals(tableDeals, refDate);
}

function applyCurrentSort() {
    if (!currentSort.column) {
        return;
    }

    const direction = currentSort.direction === 'asc' ? 1 : -1;

    filteredDeals.sort((a, b) => {
        const valueA = getSortableValue(a, currentSort.column);
        const valueB = getSortableValue(b, currentSort.column);

        if (typeof valueA === 'number' || typeof valueB === 'number') {
            return compareNumbers(valueA, valueB) * direction;
        }

        return cleanString(valueA).localeCompare(cleanString(valueB), undefined, {
            numeric: true,
            sensitivity: 'base'
        }) * direction;
    });
}

function getSortableValue(deal, column) {
    if (column === 'Amount') {
        return Number.isFinite(deal.AmountValue) ? deal.AmountValue : Number.NEGATIVE_INFINITY;
    }

    if (column === 'Date_Captured') {
        return Number.isFinite(deal.DateValue) ? deal.DateValue : Number.NEGATIVE_INFINITY;
    }

    return deal[column] || '';
}

function getSortIcon(column) {
    if (currentSort.column !== column) {
        return '⇅';
    }

    return currentSort.direction === 'asc' ? '↑' : '↓';
}

function downloadFilteredCsv() {
    const refDate = getReferenceDate(filteredDeals);
    const tableDeals = getRecentDeals(filteredDeals, refDate, 7);

    if (!tableDeals.length) {
        window.alert('There are no deals in the last 7 days to export.');
        return;
    }

    const headers = TABLE_COLUMNS.map(column => column.label);
    const rows = tableDeals.map(deal => TABLE_COLUMNS.map(column => {
        if (column.key === 'Date_Captured') {
            return cleanString(deal.Date_Captured);
        }

        return cleanString(deal[column.key]);
    }));

    const csvContent = [headers, ...rows]
        .map(row => row.map(escapeCsvValue).join(','))
        .join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const downloadLink = document.createElement('a');
    const referenceDate = getReferenceDate(filteredDeals);

    downloadLink.href = url;
    downloadLink.download = `funding-deals-${formatDateInputValue(referenceDate)}.csv`;
    document.body.appendChild(downloadLink);
    downloadLink.click();
    downloadLink.remove();
    URL.revokeObjectURL(url);
}

async function updateLastUpdated(manifest) {
    let lastUpdated = cleanString(manifest.last_updated);

    if (!lastUpdated) {
        try {
            const response = await fetch(LAST_UPDATED_PATH, { cache: 'no-store' });
            if (response.ok) {
                lastUpdated = (await response.text()).trim();
            }
        } catch (error) {
            console.log('Could not load last_updated.txt');
        }
    }

    const syncReferenceDate = getMostRecentDate([
        parseSyncTimestamp(lastUpdated),
        parseSyncTimestamp(manifest.generated_at)
    ]);

    let formattedLastUpdated = formatSyncTimestamp(syncReferenceDate);

    if (!formattedLastUpdated) {
        formattedLastUpdated = formatSyncTimestamp(lastUpdated);
    }

    if (!formattedLastUpdated && manifest.generated_at) {
        formattedLastUpdated = formatSyncTimestamp(manifest.generated_at);
    }

    if (!formattedLastUpdated) {
        return;
    }

    const lastUpdatedElem = document.getElementById('last-updated');
    const syncElem = document.getElementById('sync-time');

    if (lastUpdatedElem) {
        lastUpdatedElem.textContent = `Last updated: ${formattedLastUpdated}`;
    }

    if (syncElem) {
        syncElem.textContent = formattedLastUpdated;
    }
}

function formatCurrencyCompact(num) {
    if (!Number.isFinite(num) || num <= 0) {
        return '$0';
    }

    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        notation: 'compact',
        maximumFractionDigits: 2
    }).format(num);
}

function formatAmountInputValue(num) {
    if (!Number.isFinite(num)) {
        return '';
    }

    return `$${formatLargeNumber(num)}`;
}

function formatDealAmountForTable(deal) {
    if (Number.isFinite(deal.AmountOriginalValue)) {
        const currency = cleanString(deal.AmountCurrency) || 'USD';
        return `${currency} ${formatLargeNumber(deal.AmountOriginalValue)}`;
    }

    return cleanString(deal.Amount) || 'N/A';
}

function formatLargeNumber(num) {
    if (!Number.isFinite(num)) {
        return '0';
    }

    if (num >= 1e9) return `${trimTrailingZeros((num / 1e9).toFixed(2))}B`;
    if (num >= 1e6) return `${trimTrailingZeros((num / 1e6).toFixed(2))}M`;
    if (num >= 1e3) return `${trimTrailingZeros((num / 1e3).toFixed(2))}K`;
    return trimTrailingZeros(num.toFixed(0));
}

function formatDate(value) {
    if (!value) {
        return 'N/A';
    }

    const timestamp = value instanceof Date ? value.getTime() : parseDateValue(value);
    if (!Number.isFinite(timestamp)) {
        return cleanString(value) || 'N/A';
    }

    return new Intl.DateTimeFormat('en-US', {
        timeZone: 'UTC',
        year: 'numeric',
        month: 'short',
        day: 'numeric'
    }).format(new Date(timestamp));
}

function formatDateRange(startDate, endDate) {
    const startLabel = formatDate(startDate);
    const endLabel = formatDate(endDate);
    return startLabel === endLabel ? startLabel : `${startLabel} - ${endLabel}`;
}

function parseAmount(value) {
    return parseAmountInfo(value).usdValue;
}

function parseAmountInfo(value) {
    const raw = cleanString(value);
    if (!raw) {
        return {
            usdValue: null,
            currency: '',
            originalValue: null,
            isConverted: false
        };
    }

    const normalized = raw.toLowerCase().replace(/,/g, '').trim();

    if (['unknown', 'undisclosed', 'not disclosed', 'n/a', 'na', '-', 'nil'].includes(normalized)) {
        return {
            usdValue: null,
            currency: '',
            originalValue: null,
            isConverted: false
        };
    }

    const explicitUsdCandidates = [
        ...matchAmountCandidates(raw, /(?:US\$|USD|\$)\s*\d[\d,]*(?:\.\d+)?(?:\s*(?:trillion|tn|billion|bn|million|mn|thousand|k|[tmb]))?/gi),
        ...matchAmountCandidates(raw, /\d[\d,]*(?:\.\d+)?(?:\s*(?:trillion|tn|billion|bn|million|mn|thousand|k|[tmb]))?\s*(?:US\$|USD)/gi)
    ];

    for (const candidate of explicitUsdCandidates) {
        const parsedCandidate = parseAmountCandidate(candidate);
        if (Number.isFinite(parsedCandidate)) {
            return {
                usdValue: parsedCandidate,
                currency: 'USD',
                originalValue: parsedCandidate,
                isConverted: false
            };
        }
    }
    const currency = detectAmountCurrency(raw);
    const genericCandidates = matchAmountCandidates(raw, /\d[\d,]*(?:\.\d+)?(?:\s*(?:trillion|tn|billion|bn|million|mn|thousand|k|[tmb]))?/gi);

    for (const candidate of genericCandidates) {
        const parsedCandidate = parseAmountCandidate(candidate);
        if (Number.isFinite(parsedCandidate)) {
            const usdValue = convertAmountToUsd(parsedCandidate, currency || 'USD');
            return {
                usdValue,
                currency: currency || 'USD',
                originalValue: parsedCandidate,
                isConverted: Boolean(currency && currency !== 'USD')
            };
        }
    }

    return {
        usdValue: null,
        currency,
        originalValue: null,
        isConverted: false
    };
}

function matchAmountCandidates(value, pattern) {
    return [...cleanString(value).matchAll(pattern)].map(match => match[0]);
}

function parseAmountCandidate(value) {
    const normalized = cleanString(value).toLowerCase().replace(/,/g, '').trim();
    const numberMatch = normalized.match(/(\d+(?:\.\d+)?)/);

    if (!numberMatch) {
        return null;
    }

    let numericValue = Number(numberMatch[1]);
    if (!Number.isFinite(numericValue)) {
        return null;
    }

    if (/\btrillion\b/.test(normalized) || /\btn\b/.test(normalized) || /(?<![a-z])t(?![a-z])/.test(normalized)) {
        numericValue *= 1e12;
    } else if (/\bbillion\b/.test(normalized) || /\bbn\b/.test(normalized) || /(?<![a-z])b(?![a-z])/.test(normalized)) {
        numericValue *= 1e9;
    } else if (/\bmillion\b/.test(normalized) || /\bmn\b/.test(normalized) || /(?<![a-z])m(?![a-z])/.test(normalized)) {
        numericValue *= 1e6;
    } else     if (/\bthousand\b/.test(normalized) || /(?<![a-z])k(?![a-z])/.test(normalized)) {
        numericValue *= 1e3;
    }

    if (/\bcrore\b|\bcr\b/.test(normalized)) {
        numericValue *= 1e7;
    }
    if (/\blakh\b|\blac\b/.test(normalized)) {
        numericValue *= 1e5;
    }

    return numericValue;
}

function convertAmountToUsd(value, currency) {
    const rate = currencyToUsdRate[currency] || null;

    if (!Number.isFinite(value)) {
        return null;
    }

    if (!currency || currency === 'USD') {
        return value;
    }

    if (!Number.isFinite(rate)) {
        return null;
    }

    return value * rate;
}

function detectAmountCurrency(value) {
    const cleaned = cleanString(value).toUpperCase();

    if (!cleaned) {
        return '';
    }

    if (/(?:US\$|USD|\$)/.test(cleaned)) {
        return 'USD';
    }

    const currencyMatchers = [
        ['AED', /\bAED\b/],
        ['AUD', /\bAUD\b/],
        ['CAD', /\bCAD\b/],
        ['CNY', /\bCNY\b/],
        ['DKK', /\bDKK\b/],
        ['EUR', /\bEUR\b|€/],
        ['GBP', /\bGBP\b|£/],
        ['ILS', /\bILS\b/],
        ['INR', /\bINR\b|₹/],
        ['JPY', /\bJPY\b|¥/],
        ['KRW', /\bKRW\b|₩/],
        ['SEK', /\bSEK\b/],
        ['SGD', /\bSGD\b/],
        ['USDC', /\bUSDC\b/],
        ['ZAR', /\bZAR\b/]
    ];

    for (const [currency, matcher] of currencyMatchers) {
        if (matcher.test(cleaned)) {
            return currency;
        }
    }

    return '';
}

function getDealExclusionReason(deal) {
    if (Number.isFinite(deal.AmountValue) && deal.AmountValue > MAX_REALISTIC_DEAL_AMOUNT) {
        return `Amount exceeds safety cap of ${formatCurrencyCompact(MAX_REALISTIC_DEAL_AMOUNT)}`;
    }

    if (Number.isFinite(deal.AmountValue) && deal.AmountValue > 0 && deal.AmountValue < MIN_REALISTIC_DEAL_AMOUNT) {
        return `Amount below realism floor of ${formatCurrencyCompact(MIN_REALISTIC_DEAL_AMOUNT)}`;
    }

    return '';
}

function parseSyncTimestamp(value) {
    const cleaned = cleanString(value);

    if (!cleaned) {
        return null;
    }

    const basicTimestampMatch = cleaned.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/);
    if (basicTimestampMatch) {
        const [, year, month, day, hour, minute, second = '00'] = basicTimestampMatch;
        return createDateInTimeZone({
            year: Number(year),
            month: Number(month),
            day: Number(day),
            hour: Number(hour),
            minute: Number(minute),
            second: Number(second)
        }, SYNC_TIME_ZONE) || new Date(Date.UTC(
            Number(year),
            Number(month) - 1,
            Number(day),
            Number(hour),
            Number(minute),
            Number(second)
        ));
    }

    const parsed = new Date(cleaned);
    return Number.isFinite(parsed.getTime()) ? parsed : null;
}

function formatSyncTimestamp(value) {
    const parsed = value instanceof Date ? value : parseSyncTimestamp(value);

    if (!parsed) {
        return '';
    }

    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: SYNC_TIME_ZONE,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hourCycle: 'h23'
    }).formatToParts(parsed).reduce((acc, part) => {
        acc[part.type] = part.value;
        return acc;
    }, {});

    return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute} ${SYNC_TIME_ZONE_LABEL}`;
}

function createDateInTimeZone(parts, timeZone) {
    const utcGuess = Date.UTC(
        parts.year,
        parts.month - 1,
        parts.day,
        parts.hour,
        parts.minute,
        parts.second || 0
    );

    let candidate = new Date(utcGuess - getTimeZoneOffsetMs(new Date(utcGuess), timeZone));

    if (!matchesTimeZoneParts(candidate, parts, timeZone)) {
        candidate = new Date(utcGuess - getTimeZoneOffsetMs(candidate, timeZone));
    }

    return Number.isFinite(candidate.getTime()) ? candidate : null;
}

function getTimeZoneOffsetMs(date, timeZone) {
    const parts = getTimeZoneDateParts(date, timeZone);
    const asUtc = Date.UTC(
        Number(parts.year),
        Number(parts.month) - 1,
        Number(parts.day),
        Number(parts.hour),
        Number(parts.minute),
        Number(parts.second)
    );
    return asUtc - date.getTime();
}

function getTimeZoneDateParts(date, timeZone) {
    return new Intl.DateTimeFormat('en-CA', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hourCycle: 'h23'
    }).formatToParts(date).reduce((acc, part) => {
        acc[part.type] = part.value;
        return acc;
    }, {});
}

function matchesTimeZoneParts(date, expectedParts, timeZone) {
    const actualParts = getTimeZoneDateParts(date, timeZone);
    return (
        Number(actualParts.year) === expectedParts.year &&
        Number(actualParts.month) === expectedParts.month &&
        Number(actualParts.day) === expectedParts.day &&
        Number(actualParts.hour) === expectedParts.hour &&
        Number(actualParts.minute) === expectedParts.minute &&
        Number(actualParts.second) === (expectedParts.second || 0)
    );
}

function getMostRecentDate(dates) {
    const validDates = dates.filter(date => date instanceof Date && Number.isFinite(date.getTime()));

    if (!validDates.length) {
        return null;
    }

    return new Date(Math.max(...validDates.map(date => date.getTime())));
}

function parseDateValue(value) {
    const cleaned = cleanString(value);
    if (!cleaned) {
        return null;
    }

    if (/^\d+$/.test(cleaned) && cleaned.length >= 12) {
        const epochMs = Number(cleaned);
        return Number.isFinite(epochMs) ? epochMs : null;
    }

    const timestamp = Date.parse(cleaned.includes('T') ? cleaned : `${cleaned}T00:00:00Z`);
    return Number.isFinite(timestamp) ? timestamp : null;
}

function normalizeCategoryValue(value) {
    const cleaned = cleanString(value);
    if (!hasMeaningfulValue(cleaned)) return UNKNOWN_LABEL;
    return normalizeRoundToGroup(cleaned);
}

function normalizeRoundToGroup(raw) {
    const lower = raw.toLowerCase();

    if (/acceleration/i.test(lower)) return 'Acceleration';

    if (/\bpre[- ]?seed\b/.test(lower) && !/series\s*a/i.test(lower) && !/seed\/pre-series/i.test(lower)) {
        return 'Pre-Seed';
    }

    if (/\bpre[- ]?series\s*a\b/.test(lower) || /seed\/pre-series/i.test(lower)) {
        return 'Pre-Series A';
    }

    if (/series\s*[d-z](?!\w)/i.test(lower) && !/series\s*unknown/i.test(lower)) return 'Series D+';
    if (/series\s*c/i.test(lower)) return 'Series C';
    if (/series\s*b/i.test(lower)) return 'Series B';
    if (/series\s*a/i.test(lower) || /seed\/series\s*a/i.test(lower)) return 'Series A';

    if (/\bseed\b/.test(lower)) return 'Seed';

    if (/growth|late[r ]?\s*stage|unicorn/i.test(lower)) return 'Growth/Late Stage';
    if (/bridge/i.test(lower)) return 'Bridge';
    if (/\bdebt\b/i.test(lower)) return 'Debt';
    if (/\bgrant\b/i.test(lower)) return 'Grant';
    if (/strategic/i.test(lower)) return 'Strategic';
    if (/venture/i.test(lower)) return 'Venture/Other';

    if (/funding|financing|investment|round|new\b/i.test(lower)) return 'Other';

    return 'Other';
}

function normalizeTierValue(value) {
    const cleaned = cleanString(value);
    return hasMeaningfulValue(cleaned) ? cleaned : UNKNOWN_LABEL;
}

function normalizeHiringValue(value) {
    const cleaned = cleanString(value);
    const normalized = cleaned.toLowerCase();

    if (!cleaned || ['unknown', 'n/a', 'na'].includes(normalized)) {
        return UNKNOWN_LABEL;
    }

    if (
        normalized.includes('not hiring') ||
        normalized.includes('no hiring') ||
        normalized.includes('not actively hiring') ||
        normalized === 'no' ||
        normalized === 'false'
    ) {
        return 'No';
    }

    if (
        normalized.includes('hiring') ||
        normalized.includes('open role') ||
        normalized.includes('actively hiring') ||
        normalized === 'yes' ||
        normalized === 'true'
    ) {
        return 'Yes';
    }

    return UNKNOWN_LABEL;
}

function getHiringBadgeClass(hiringValue) {
    if (hiringValue === 'Yes') {
        return 'positive';
    }

    if (hiringValue === 'No') {
        return 'negative';
    }

    return 'neutral';
}

function hasMeaningfulValue(value) {
    const cleaned = cleanString(value).toLowerCase();

    return ![
        '',
        'n/a',
        'na',
        'none',
        'unknown',
        'not disclosed',
        'not available',
        'nil',
        '-'
    ].includes(cleaned);
}

function hasUsefulLinks(value) {
    return extractUrls(value).length > 0;
}

function extractUrls(value) {
    const raw = cleanString(value);
    if (!raw || !hasMeaningfulValue(raw)) {
        return [];
    }

    const matches = raw.match(/((?:https?:\/\/|www\.)[^\s,]+|(?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/[^\s,]*)?)/gi) || [];
    return [...new Set(matches.map(normalizeUrl).filter(Boolean))];
}

function normalizeUrl(url) {
    const cleaned = cleanString(url).replace(/[),.;]+$/g, '');
    if (!cleaned) {
        return '';
    }

    if (/^https?:\/\//i.test(cleaned)) {
        return cleaned;
    }

    if (/^(?:www\.)/i.test(cleaned) || /^(?:[a-z0-9-]+\.)+[a-z]{2,}/i.test(cleaned)) {
        return `https://${cleaned}`;
    }

    return '';
}

function linkifyText(value) {
    const text = cleanString(value);

    if (!text) {
        return '<span class="cell-muted">N/A</span>';
    }

    const urlPattern = /((?:https?:\/\/|www\.)[^\s,]+|(?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/[^\s,]*)?)/gi;
    let lastIndex = 0;
    let output = '';
    let match;

    while ((match = urlPattern.exec(text)) !== null) {
        const [matchedText] = match;
        const normalizedUrl = normalizeUrl(matchedText);

        output += escapeHtml(text.slice(lastIndex, match.index));

        if (normalizedUrl) {
            output += `<a href="${escapeHtml(normalizedUrl)}" target="_blank" rel="noopener noreferrer" class="text-link">${escapeHtml(matchedText)}</a>`;
        } else {
            output += escapeHtml(matchedText);
        }

        lastIndex = match.index + matchedText.length;
    }

    output += escapeHtml(text.slice(lastIndex));
    return output;
}

function getLinkLabel(url, fallbackLabel, index) {
    try {
        const parsed = new URL(url);
        const host = parsed.hostname.replace(/^www\./i, '');
        return host || `${fallbackLabel} ${index + 1}`;
    } catch (error) {
        return `${fallbackLabel} ${index + 1}`;
    }
}

function getTopCategory(values) {
    const counts = new Map();

    values
        .filter(value => hasMeaningfulValue(value) && value !== UNKNOWN_LABEL)
        .forEach(value => {
            counts.set(value, (counts.get(value) || 0) + 1);
        });

    if (!counts.size) {
        return UNKNOWN_LABEL;
    }

    return [...counts.entries()]
        .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], undefined, { sensitivity: 'base' }))[0][0];
}

function getMedian(values) {
    if (!values.length) {
        return 0;
    }

    const sorted = [...values].sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);

    if (sorted.length % 2 === 0) {
        return (sorted[middle - 1] + sorted[middle]) / 2;
    }

    return sorted[middle];
}

function getCountryFlag(nation) {
    const flags = {
        Britain: '🇬🇧',
        USA: '🇺🇸',
        Canada: '🇨🇦',
        India: '🇮🇳',
        Israel: '🇮🇱',
        Singapore: '🇸🇬',
        UAE: '🇦🇪',
        Dubai: '🇦🇪',
        Dubai_UAE: '🇦🇪',
        MENA: '🌍'
    };

    return flags[nation] || '🌍';
}

function startOfUtcDay(date) {
    return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function endOfUtcDay(date) {
    return new Date(startOfUtcDay(date).getTime() + (24 * 60 * 60 * 1000) - 1);
}

function startOfUtcWeek(date) {
    const day = date.getUTCDay() || 7;
    const diff = day - 1;
    return startOfUtcDay(new Date(date.getTime() - (diff * 24 * 60 * 60 * 1000)));
}

function endOfUtcWeek(date) {
    return new Date(startOfUtcWeek(date).getTime() + (7 * 24 * 60 * 60 * 1000) - 1);
}

function startOfUtcMonth(date) {
    return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));
}

function endOfUtcMonth(date) {
    return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1) - 1);
}

function showLoading(show) {
    const content = document.getElementById('dashboard-content');

    if (!content) {
        return;
    }

    let loader = document.getElementById('loading-indicator');

    if (show) {
        if (!loader) {
            loader = document.createElement('div');
            loader.id = 'loading-indicator';
            loader.className = 'loading loading-inline';
            loader.textContent = 'Loading data...';
            content.prepend(loader);
        }
        content.classList.add('is-loading');
    } else {
        if (loader) {
            loader.remove();
        }
        content.classList.remove('is-loading');
    }
}

function showError(message) {
    const content = document.getElementById('dashboard-content');

    if (!content) {
        return;
    }

    showLoading(false);
    content.innerHTML = `<div class="error">${escapeHtml(message)}</div>`;
}

function uniqueValues(values) {
    return [...new Set(values.filter(Boolean))];
}

function sortAlphabetically(values) {
    return [...values].sort((a, b) =>
        a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' })
    );
}

function sortWithUnknownLast(values) {
    return [...values].sort((a, b) => {
        const idxA = ROUND_GROUP_ORDER.indexOf(a);
        const idxB = ROUND_GROUP_ORDER.indexOf(b);
        if (idxA !== -1 && idxB !== -1) return idxA - idxB;
        if (idxA !== -1) return -1;
        if (idxB !== -1) return 1;
        if (a === UNKNOWN_LABEL) return 1;
        if (b === UNKNOWN_LABEL) return -1;
        return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' });
    });
}

function sortTierValues(values) {
    return [...values].sort((a, b) => {
        if (a === UNKNOWN_LABEL) return 1;
        if (b === UNKNOWN_LABEL) return -1;

        const tierMatchA = a.match(/tier\s*(\d+)/i);
        const tierMatchB = b.match(/tier\s*(\d+)/i);

        if (tierMatchA && tierMatchB) {
            return Number(tierMatchA[1]) - Number(tierMatchB[1]);
        }

        return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' });
    });
}

function sortByPreferredOrder(values, preferredOrder) {
    return [...values].sort((a, b) => {
        const indexA = preferredOrder.indexOf(a);
        const indexB = preferredOrder.indexOf(b);

        if (indexA !== -1 && indexB !== -1) {
            return indexA - indexB;
        }

        if (indexA !== -1) return -1;
        if (indexB !== -1) return 1;
        return a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' });
    });
}

function getRangeStep(min, max) {
    const spread = Math.max(max - min, 1);
    return Math.max(Math.round(spread / 300), 1);
}

function amountToSliderValue(amount) {
    if (!amountRange.hasValues) {
        return 0;
    }

    if (amountRange.availableMax <= amountRange.availableMin) {
        return 0;
    }

    const scaleMin = Math.log10(amountRange.availableMin + 1);
    const scaleMax = Math.log10(amountRange.availableMax + 1);
    const scaleValue = Math.log10(clamp(amount, amountRange.availableMin, amountRange.availableMax) + 1);
    const ratio = (scaleValue - scaleMin) / Math.max(scaleMax - scaleMin, Number.EPSILON);

    return clamp(Math.round(ratio * AMOUNT_SLIDER_MAX), 0, AMOUNT_SLIDER_MAX);
}

function sliderValueToAmount(sliderValue) {
    if (!amountRange.hasValues) {
        return 0;
    }

    if (amountRange.availableMax <= amountRange.availableMin) {
        return amountRange.availableMin;
    }

    const ratio = clamp(sliderValue, 0, AMOUNT_SLIDER_MAX) / AMOUNT_SLIDER_MAX;
    const scaleMin = Math.log10(amountRange.availableMin + 1);
    const scaleMax = Math.log10(amountRange.availableMax + 1);
    const scaleValue = scaleMin + ((scaleMax - scaleMin) * ratio);
    const rawAmount = (10 ** scaleValue) - 1;

    return roundSliderAmount(rawAmount);
}

function roundSliderAmount(value) {
    const clampedValue = clamp(value, amountRange.availableMin, amountRange.availableMax);

    if (clampedValue >= 1e9) {
        return Math.round(clampedValue / 1e7) * 1e7;
    }

    if (clampedValue >= 1e6) {
        return Math.round(clampedValue / 1e5) * 1e5;
    }

    if (clampedValue >= 1e3) {
        return Math.round(clampedValue / 1e3) * 1e3;
    }

    return Math.round(clampedValue);
}

function compareNumbers(a, b) {
    const valueA = Number.isFinite(a) ? a : Number.NEGATIVE_INFINITY;
    const valueB = Number.isFinite(b) ? b : Number.NEGATIVE_INFINITY;
    return valueA - valueB;
}

function clamp(value, min, max) {
    return Math.min(Math.max(value, min), max);
}

function cleanString(value) {
    return value == null ? '' : String(value).trim();
}

function formatDateInputValue(date) {
    return date.toISOString().split('T')[0];
}

function escapeCsvValue(value) {
    const stringValue = cleanString(value);
    return `"${stringValue.replace(/"/g, '""')}"`;
}

function trimTrailingZeros(value) {
    return String(value).replace(/\.0+$|(\.\d*[1-9])0+$/g, '$1');
}

function toTitleCase(text) {
    return text.replace(/\w\S*/g, word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase());
}

function escapeHtml(value) {
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function insightSortableTh(tableKey, label, colIdx, sortType = 'string') {
    const st = insightTableSort[tableKey];
    const icon = insightSortGlyph(st ? st.col : -1, st ? st.dir : 'asc', colIdx);
    return `<th scope="col" class="insight-sortable" data-sort-col="${colIdx}" data-sort-type="${escapeHtml(sortType)}">${escapeHtml(label)} <span class="insight-sort-icon">${icon}</span></th>`;
}

function insightSortGlyph(activeCol, activeDir, colIdx) {
    if (activeCol !== colIdx) return '⇅';
    return activeDir === 'asc' ? '↑' : '↓';
}

function insightRowSortAttrs(values) {
    return values.map((v, i) => {
        const raw = v === null || v === undefined ? '' : String(v);
        return `data-sort${i}="${escapeHtml(raw)}"`;
    }).join(' ');
}

function updateInsightSortHeaderIcons(tableEl, tableKey) {
    const state = insightTableSort[tableKey];
    const activeCol = state ? state.col : -1;
    const activeDir = state ? state.dir : 'asc';
    tableEl.querySelectorAll('thead th.insight-sortable').forEach(th => {
        const col = Number(th.dataset.sortCol);
        const icon = th.querySelector('.insight-sort-icon');
        if (icon) {
            icon.textContent = insightSortGlyph(activeCol, activeDir, col);
        }
    });
}

function getCellSortValue(tr, colIdx, sortType) {
    const raw = tr.getAttribute(`data-sort${colIdx}`) || '';
    if (sortType === 'number') {
        if (raw === '') {
            return Number.NEGATIVE_INFINITY;
        }
        const n = Number(raw);
        return Number.isFinite(n) ? n : Number.NEGATIVE_INFINITY;
    }
    return cleanString(raw).toLowerCase();
}

function sortInsightTableBody(tableEl, tableKey) {
    const state = insightTableSort[tableKey];
    if (!state) return;

    const tbody = tableEl.querySelector('tbody');
    if (!tbody) return;

    const th = tableEl.querySelector(`thead th.insight-sortable[data-sort-col="${state.col}"]`);
    const sortType = (th && th.dataset.sortType) === 'number' ? 'number' : 'string';
    const dir = state.dir === 'desc' ? -1 : 1;

    const rows = [...tbody.querySelectorAll('tr')];
    rows.sort((a, b) => {
        const va = getCellSortValue(a, state.col, sortType);
        const vb = getCellSortValue(b, state.col, sortType);
        if (sortType === 'number') {
            return compareNumbers(va, vb) * dir;
        }
        return cleanString(va).localeCompare(cleanString(vb), undefined, { numeric: true, sensitivity: 'base' }) * dir;
    });
    rows.forEach(row => tbody.appendChild(row));
    updateInsightSortHeaderIcons(tableEl, tableKey);
}

function applyStoredInsightSorts(root) {
    if (!root) return;
    root.querySelectorAll('[data-insight-table] table').forEach(tableEl => {
        const wrap = tableEl.closest('[data-insight-table]');
        const key = wrap && wrap.dataset.insightTable;
        if (key && insightTableSort[key]) {
            sortInsightTableBody(tableEl, key);
        } else if (key) {
            updateInsightSortHeaderIcons(tableEl, key);
        }
    });
}

function handleInsightTableHeaderClick(th) {
    const tableEl = th.closest('table');
    const wrap = th.closest('[data-insight-table]');
    if (!tableEl || !wrap) return;

    const tableKey = wrap.dataset.insightTable;
    if (!tableKey) return;

    const col = Number(th.dataset.sortCol);
    if (!Number.isFinite(col)) return;

    const prev = insightTableSort[tableKey];
    if (prev && prev.col === col) {
        insightTableSort[tableKey] = { col, dir: prev.dir === 'asc' ? 'desc' : 'asc' };
    } else {
        insightTableSort[tableKey] = { col, dir: 'asc' };
    }
    sortInsightTableBody(tableEl, tableKey);
}

document.addEventListener('click', event => {
    const th = event.target.closest('[data-insight-table] thead th.insight-sortable');
    if (!th) return;
    event.preventDefault();
    handleInsightTableHeaderClick(th);
});

window.sortDeals = sortDeals;

function setupPageNavigation() {
    const tabs = Array.from(document.querySelectorAll('.page-tab'));
    const countryFilterGroup = document.getElementById('country-intel-filter-group');
    const countryPageTab = tabs.find(tab => tab.dataset.pageTarget === 'page-3');
    tabs.forEach(tab => {
        tab.setAttribute('role', 'tab');
        tab.setAttribute('aria-selected', tab.classList.contains('active') ? 'true' : 'false');
        tab.classList.toggle('page-tab-compact', tab !== countryPageTab);
    });
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const target = tab.dataset.pageTarget;
            if (!target) return;
            activePageId = target;
            tabs.forEach(btn => {
                const isActive = btn === tab;
                btn.classList.toggle('active', isActive);
                btn.setAttribute('aria-selected', isActive ? 'true' : 'false');
            });
            document.querySelectorAll('.dashboard-page').forEach(page => {
                page.classList.toggle('active', page.id === target);
            });
            if (countryFilterGroup) {
                countryFilterGroup.classList.toggle('hidden', target !== 'page-3');
            }
            if (target === 'page-3') {
                updateCountryContextSubtitle(selectedCountryInsight || 'All countries');
            }
            if (target === 'page-2' && leafletMapInstance) {
                // Allow the page to become visible before recalculating map dimensions
                setTimeout(() => leafletMapInstance.invalidateSize(), 50);
            }
        });
    });
    if (countryFilterGroup) {
        countryFilterGroup.classList.toggle('hidden', activePageId !== 'page-3');
    }
    updateCountryContextSubtitle(selectedCountryInsight || 'All countries');
    document.addEventListener('click', event => {
        const link = event.target.closest('.alert-link[data-target-page]');
        if (!link) return;
        event.preventDefault();
        const targetPage = cleanString(link.dataset.targetPage);
        if (!targetPage) return;
        const targetTab = tabs.find(tab => tab.dataset.pageTarget === targetPage);
        if (targetTab) {
            targetTab.click();
        }
        const anchorId = cleanString(link.dataset.targetAnchor);
        if (anchorId) {
            const anchor = document.getElementById(anchorId);
            if (anchor) {
                anchor.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
        }
    });
}

function renderEnhancedPages(deals) {
    try { renderGlobalOverviewPage(deals); } catch (err) { console.error('Error rendering Global Overview page:', err); }
    try { renderCountryIntelligencePage(deals); } catch (err) { console.error('Error rendering Country Intelligence page:', err); }
    try { renderInvestorStartupPage(deals); } catch (err) { console.error('Error rendering Investor & Startup page:', err); }
}

function renderGlobalOverviewPage(deals) {
    const container = document.getElementById('global-overview-content');
    if (!container) return;

    const allCountries = summarizeCountries(deals);
    const topCountries = allCountries.slice(0, 10);
    const investors = summarizeInvestors(deals);
    const daily = summarizeDailyDeals(deals, 14);
    const alerts = buildSeverityAlerts(deals, topCountries);
    const maturityScores = computeEcosystemMaturityScores(deals);
    const funnelData = computeStageFunnel(deals);
    const regionalData = computeRegionalDominance(deals);
    const heatSignals = computeCountryHeatSignals(deals);

    container.innerHTML = `
        <div class="insight-grid">
            <section class="insight-card span-12">
                <h2>Global Overview</h2>
                <div class="kpi-grid">
                    ${buildKpiTile('Total Deals', deals.length.toLocaleString())}
                    ${buildKpiTile('Total Funding', formatCurrencyCompact(deals.filter(d => Number.isFinite(d.AmountValue)).reduce((s, d) => s + d.AmountValue, 0)))}
                    ${buildKpiTile('Active Countries', new Set(deals.map(d => d.Nation).filter(Boolean)).size.toLocaleString())}
                    ${buildKpiTile('Unique Investors', investors.length.toLocaleString())}
                </div>
            </section>

            <section class="insight-card span-7">
                <h3>World Map (All Nations by Deal Count)</h3>
                <div id="world-map-leaflet" class="world-map-leaflet"></div>
            </section>

            <section class="insight-card span-5">
                <h3>Daily Trend (Last 14 Days)</h3>
                <div class="trend-bars">${buildDailyTrendBars(daily)}</div>
            </section>

            <section class="insight-card span-8">
                <h3>Country Leaderboard</h3>
                <div class="table-scroll-wrap" data-insight-table="leaderboard">
                    <table class="simple-table sticky-header-table">
                        <thead><tr>
                            ${insightSortableTh('leaderboard', 'Country', 0)}
                            ${insightSortableTh('leaderboard', 'Deals', 1, 'number')}
                            ${insightSortableTh('leaderboard', 'Funding', 2, 'number')}
                        </tr></thead>
                        <tbody>
                            ${allCountries.map(country => `<tr ${insightRowSortAttrs([country.name, country.deals, country.funding])}><td>${escapeHtml(country.name)}</td><td>${country.deals.toLocaleString()}</td><td>${formatCurrencyCompact(country.funding)}</td></tr>`).join('') || '<tr><td colspan="3">No data</td></tr>'}
                        </tbody>
                    </table>
                </div>
            </section>

            <section class="insight-card span-4">
                <h3>Alerts Panel</h3>
                ${renderSeverityAlerts(alerts)}
            </section>

            <section class="insight-card span-12">
                <h3>Country Heat Signals (4-Week Rolling Z-Score)</h3>
                <p class="insight-sub">Z-score of weekly deal count vs 8-week baseline. Positive = heating up, Negative = cooling down.</p>
                ${buildHeatSignalsTable(heatSignals)}
            </section>

            <section class="insight-card span-6" id="data-freshness">
                <h3>Data Freshness by Country</h3>
                <p class="insight-sub">Last captured is the calendar date of the newest deal in your current filters for that country (from each deal’s capture date). Lag is days since that date compared to the newest capture in the filtered set. It reflects pipeline timing, not whether the market had zero deals on a given day.</p>
                ${renderDataFreshnessWidget(deals)}
            </section>

            <section class="insight-card span-6" id="anomaly-audit">
                <h3>Anomaly Audit Panel</h3>
                <p class="insight-sub">Unified review of high and low unrealistic amounts from outlier.json.</p>
                ${renderAnomalyAuditPanel()}
            </section>

            <section class="insight-card span-12">
                <h3>Ecosystem Maturity Score by Country</h3>
                <p class="insight-sub">Composite score (0–100) blending deal count (25%), avg deal size (25%), sector diversity (20%), investor diversity (20%), and hiring rate (10%).</p>
                ${buildMaturityScoreTable(maturityScores)}
            </section>

            <section class="insight-card span-5">
                <h3>Stage Progression Funnel (Seed → Series A)</h3>
                <p class="insight-sub">Pipeline health: fraction of Seed-stage startups from 6+ months ago that appear again at Series A.</p>
                ${buildStageFunnel(funnelData)}
            </section>

            <section class="insight-card span-7">
                <h3>Regional Dominance — Fastest Growing Subregions</h3>
                <p class="insight-sub">Deal-count growth comparing the last 30 days vs the prior 30 days, by subregion.</p>
                ${buildRegionalDominanceChart(regionalData)}
            </section>
        </div>
    `;

    initLeafletWorldMap(allCountries);
    applyStoredInsightSorts(container);
}

// ─── Ecosystem Maturity Score ────────────────────────────────────────────────

function computeEcosystemMaturityScores(deals) {
    const countryMap = new Map();
    deals.forEach(deal => {
        const name = cleanString(deal.Nation) || UNKNOWN_LABEL;
        if (!countryMap.has(name)) {
            countryMap.set(name, { name, deals: [], investors: new Set(), sectors: new Set(), hiringCount: 0 });
        }
        const row = countryMap.get(name);
        row.deals.push(deal);
        splitInvestors(deal.Investors).forEach(inv => row.investors.add(inv));
        row.sectors.add(classifyDealSector(deal));
        if (deal.HiringFilter === 'Yes') row.hiringCount += 1;
    });

    const countries = [...countryMap.values()].filter(c => c.deals.length >= 2);
    if (!countries.length) return [];

    const raw = countries.map(c => {
        const dealCount = c.deals.length;
        const fundedDeals = c.deals.filter(d => Number.isFinite(d.AmountValue));
        const avgDealSize = fundedDeals.length ? fundedDeals.reduce((s, d) => s + d.AmountValue, 0) / fundedDeals.length : 0;
        const medianDealSize = getMedian(fundedDeals.map(d => d.AmountValue));
        const sectorDiversity = c.sectors.size;
        const investorDiversity = c.investors.size;
        const hiringRate = dealCount ? (c.hiringCount || 0) / dealCount : 0;
        return { name: c.name, dealCount, avgDealSize, medianDealSize, sectorDiversity, investorDiversity, hiringRate };
    });

    const maxDeal = Math.max(...raw.map(r => r.dealCount), 1);
    const maxAvg = Math.max(...raw.map(r => r.avgDealSize), 1);
    const maxSector = Math.max(...raw.map(r => r.sectorDiversity), 1);
    const maxInvestor = Math.max(...raw.map(r => r.investorDiversity), 1);

    return raw.map(r => {
        const score = Math.round(
            (r.dealCount / maxDeal) * 25 +
            (r.avgDealSize / maxAvg) * 25 +
            (r.sectorDiversity / maxSector) * 20 +
            (r.investorDiversity / maxInvestor) * 20 +
            r.hiringRate * 10
        );
        return {
            name: r.name,
            score,
            dealCount: r.dealCount,
            avgDealSize: r.avgDealSize,
            medianDealSize: r.medianDealSize,
            sectorDiversity: r.sectorDiversity,
            investorDiversity: r.investorDiversity,
            hiringRate: r.hiringRate
        };
    }).sort((a, b) => b.score - a.score);
}

function classifyDealSector(deal) {
    const text = `${cleanString(deal.Description)} ${cleanString(deal.Startup_Name)}`.toLowerCase();
    if (/genai|llm|foundation model|chatbot|language model/.test(text)) return 'GenAI';
    if (/infrastructure|compute|cloud|gpu|chip|semiconductor/.test(text)) return 'Infrastructure';
    if (/health|biotech|clinical|medtech/.test(text)) return 'Healthcare AI';
    if (/enterprise|workflow|saas|automation/.test(text)) return 'Enterprise AI';
    if (/robot|autonomous|drone/.test(text)) return 'Robotics';
    if (/fintech|bank|payments|insurance/.test(text)) return 'Fintech AI';
    if (/security|cyber/.test(text)) return 'Security AI';
    if (/climate|energy|sustainab/.test(text)) return 'Climate AI';
    return 'Other AI';
}

function buildMaturityScoreTable(scores) {
    if (!scores.length) return '<p>Not enough country data to compute maturity scores.</p>';
    const rows = scores.map((s, i) => {
        const rank = i + 1;
        const hiringPct = Math.round(s.hiringRate * 100);
        return `
        <tr ${insightRowSortAttrs([rank, s.name, s.score, s.dealCount, s.avgDealSize, s.medianDealSize, s.sectorDiversity, s.investorDiversity, hiringPct])}>
            <td><strong>${rank}</strong></td>
            <td>${escapeHtml(s.name)}</td>
            <td>
                <div class="maturity-bar-wrap">
                    <div class="maturity-bar" style="width:${s.score}%"></div>
                    <span class="maturity-score-label">${s.score}</span>
                </div>
            </td>
            <td>${s.dealCount}</td>
            <td>${formatCurrencyCompact(s.avgDealSize)}</td>
            <td>${formatCurrencyCompact(s.medianDealSize)}</td>
            <td>${s.sectorDiversity}</td>
            <td>${s.investorDiversity}</td>
            <td>${hiringPct}%</td>
        </tr>`;
    }).join('');
    return `
        <div class="table-scroll-wrap" data-insight-table="maturity">
            <table class="simple-table maturity-table sticky-header-table">
                <thead><tr>
                    ${insightSortableTh('maturity', '#', 0, 'number')}
                    ${insightSortableTh('maturity', 'Country', 1)}
                    ${insightSortableTh('maturity', 'Score (0–100)', 2, 'number')}
                    ${insightSortableTh('maturity', 'Deals', 3, 'number')}
                    ${insightSortableTh('maturity', 'Avg Size', 4, 'number')}
                    ${insightSortableTh('maturity', 'Median Size', 5, 'number')}
                    ${insightSortableTh('maturity', 'Sectors', 6, 'number')}
                    ${insightSortableTh('maturity', 'Investors', 7, 'number')}
                    ${insightSortableTh('maturity', 'Hiring Rate', 8, 'number')}
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </div>`;
}

// ─── Stage Progression Funnel ────────────────────────────────────────────────

const SEED_ROUNDS = new Set(['Pre-Seed', 'Seed']);
const SERIES_A_ROUNDS = new Set(['Series A']);
const SERIES_B_PLUS_ROUNDS = new Set(['Series B', 'Series C', 'Series D+', 'Growth/Late Stage']);
const SEED_MATURITY_PERIOD_DAYS = 182;

function computeStageFunnel(deals) {
    const refDate = getReferenceDate(deals);
    const sixMonthsAgoMs = refDate.getTime() - (SEED_MATURITY_PERIOD_DAYS * 24 * 60 * 60 * 1000);

    const seedStartups = new Set(
        deals
            .filter(d => SEED_ROUNDS.has(d.RoundFilter) && Number.isFinite(d.DateValue) && d.DateValue <= sixMonthsAgoMs)
            .map(d => cleanString(d.Startup_Name).toLowerCase())
            .filter(Boolean)
    );

    const seriesAStartups = new Set(
        deals
            .filter(d => SERIES_A_ROUNDS.has(d.RoundFilter))
            .map(d => cleanString(d.Startup_Name).toLowerCase())
            .filter(Boolean)
    );

    const seriesBPlusStartups = new Set(
        deals
            .filter(d => SERIES_B_PLUS_ROUNDS.has(d.RoundFilter))
            .map(d => cleanString(d.Startup_Name).toLowerCase())
            .filter(Boolean)
    );

    const seedCount = seedStartups.size;
    const progressedToA = [...seedStartups].filter(s => seriesAStartups.has(s)).length;
    const progressedToBPlus = [...seedStartups].filter(s => seriesBPlusStartups.has(s)).length;
    const conversionRate = seedCount ? Math.round((progressedToA / seedCount) * 100) : 0;

    return { seedCount, progressedToA, progressedToBPlus, conversionRate };
}

function buildStageFunnel(data) {
    const { seedCount, progressedToA, progressedToBPlus, conversionRate } = data;
    if (!seedCount) return '<p>Not enough historical Seed data available for funnel analysis.</p>';

    const stages = [
        { label: 'Seed (6m+ ago)', count: seedCount, pct: 100, cssClass: 'funnel-bar-seed' },
        { label: 'Reached Series A', count: progressedToA, pct: seedCount ? Math.round((progressedToA / seedCount) * 100) : 0, cssClass: 'funnel-bar-series-a' },
        { label: 'Reached Series B+', count: progressedToBPlus, pct: seedCount ? Math.round((progressedToBPlus / seedCount) * 100) : 0, cssClass: 'funnel-bar-series-b' }
    ];

    const stageRows = stages.map(s => `
        <div class="funnel-stage">
            <div class="funnel-bar-wrap">
                <div class="funnel-bar ${escapeHtml(s.cssClass)}" style="width:${s.pct}%;"></div>
            </div>
            <div class="funnel-labels">
                <span class="funnel-name">${escapeHtml(s.label)}</span>
                <span class="funnel-count">${s.count.toLocaleString()} <em>(${s.pct}%)</em></span>
            </div>
        </div>`).join('');

    return `
        <div class="funnel-summary">Seed → Series A conversion rate: <strong>${conversionRate}%</strong></div>
        <div class="funnel-chart">${stageRows}</div>`;
}

// ─── Regional Dominance ──────────────────────────────────────────────────────

const COUNTRY_SUBREGION = {
    'India': 'South Asia', 'Pakistan': 'South Asia', 'Bangladesh': 'South Asia', 'Sri Lanka': 'South Asia', 'Nepal': 'South Asia',
    'China': 'East Asia', 'Japan': 'East Asia', 'South Korea': 'East Asia', 'Taiwan': 'East Asia', 'Hong Kong': 'East Asia',
    'Singapore': 'Southeast Asia', 'Indonesia': 'Southeast Asia', 'Malaysia': 'Southeast Asia', 'Vietnam': 'Southeast Asia', 'Thailand': 'Southeast Asia', 'Philippines': 'Southeast Asia',
    'USA': 'North America', 'United States': 'North America', 'Canada': 'North America', 'Mexico': 'North America',
    'United Kingdom': 'Western Europe', 'Britain': 'Western Europe', 'Germany': 'Western Europe', 'France': 'Western Europe', 'Netherlands': 'Western Europe', 'Sweden': 'Western Europe', 'Switzerland': 'Western Europe', 'Spain': 'Western Europe', 'Italy': 'Western Europe', 'Denmark': 'Western Europe', 'Finland': 'Western Europe', 'Norway': 'Western Europe',
    'Israel': 'Middle East', 'UAE': 'Middle East', 'Dubai': 'Middle East', 'Saudi Arabia': 'Middle East', 'Qatar': 'Middle East', 'Turkey': 'Middle East', 'MENA': 'Middle East',
    'Brazil': 'Latin America', 'Argentina': 'Latin America', 'Chile': 'Latin America', 'Colombia': 'Latin America',
    'Australia': 'Oceania', 'New Zealand': 'Oceania',
    'Nigeria': 'Africa', 'Kenya': 'Africa', 'South Africa': 'Africa', 'Egypt': 'Africa'
};

function computeRegionalDominance(deals) {
    const refDate = getReferenceDate(deals);
    const now = refDate.getTime();
    const thirtyDaysMs = 30 * 24 * 60 * 60 * 1000;
    const recentStart = now - thirtyDaysMs;
    const priorStart = now - 2 * thirtyDaysMs;

    const regionMap = new Map();
    deals.forEach(deal => {
        const region = COUNTRY_SUBREGION[deal.Nation] || 'Other';
        if (!regionMap.has(region)) regionMap.set(region, { region, recent: 0, prior: 0 });
        const row = regionMap.get(region);
        if (Number.isFinite(deal.DateValue)) {
            if (deal.DateValue >= recentStart) row.recent += 1;
            else if (deal.DateValue >= priorStart) row.prior += 1;
        }
    });

    return [...regionMap.values()]
        .map(r => {
            const growth = r.prior === 0
                ? (r.recent > 0 ? 100 : 0)
                : Math.round(((r.recent - r.prior) / r.prior) * 100);
            return { ...r, growth };
        })
        .filter(r => r.recent > 0 || r.prior > 0)
        .sort((a, b) => b.growth - a.growth);
}

function buildRegionalDominanceChart(regions) {
    if (!regions.length) return '<p>No regional data available.</p>';
    const maxDeals = Math.max(...regions.map(r => Math.max(r.recent, r.prior)), 1);
    const rows = regions.map(r => {
        const growthClass = r.growth > 0 ? 'growth-positive' : (r.growth < 0 ? 'growth-negative' : 'growth-neutral');
        const growthLabel = r.growth > 0 ? `+${r.growth}%` : `${r.growth}%`;
        const recentPct = Math.max(2, Math.round((r.recent / maxDeals) * 100));
        const priorPct = Math.max(2, Math.round((r.prior / maxDeals) * 100));
        return `
            <div class="region-row">
                <div class="region-name">${escapeHtml(r.region)}</div>
                <div class="region-bars">
                    <div class="region-bar region-bar-recent" style="width:${recentPct}%" title="Last 30d: ${r.recent} deals"></div>
                    <div class="region-bar region-bar-prior" style="width:${priorPct}%" title="Prior 30d: ${r.prior} deals"></div>
                </div>
                <div class="region-stats">
                    <span class="region-deals">${r.recent} <em>vs</em> ${r.prior}</span>
                    <span class="region-growth ${escapeHtml(growthClass)}">${escapeHtml(growthLabel)}</span>
                </div>
            </div>`;
    }).join('');

    return `
        <div class="region-legend">
            <span class="legend-dot legend-recent"></span>Last 30d &nbsp;
            <span class="legend-dot legend-prior"></span>Prior 30d
        </div>
        <div class="region-chart">${rows}</div>`;
}

function renderCountryIntelligencePage(deals) {
    const container = document.getElementById('country-intelligence-content');
    if (!container) return;

    const countries = summarizeCountries(deals);
    if (!countries.length) {
        container.innerHTML = '<div class="insight-card"><p>No country-level data for current filters.</p></div>';
        return;
    }

    if (!selectedCountryInsight || !countries.find(c => c.name === selectedCountryInsight)) {
        selectedCountryInsight = countries[0].name;
    }

    const countryDeals = deals.filter(deal => deal.Nation === selectedCountryInsight);
    const countryFunding = countryDeals.filter(d => Number.isFinite(d.AmountValue)).reduce((s, d) => s + d.AmountValue, 0);
    const monthlyTrend = summarizeMonthlyFunding(countryDeals, 6);
    const roundMix = summarizeRoundMix(countryDeals).slice(0, 8);
    const sectorMix = summarizeSectors(countryDeals).slice(0, 9);
    const topStartups = [...countryDeals]
        .sort((a, b) => (b.AmountValue || 0) - (a.AmountValue || 0))
        .slice(0, 10);

    const velocity = computeDealVelocity(deals, selectedCountryInsight);
    const sectorVelocity = computeSectorVelocity(countryDeals);
    const firstTimers = findFirstTimeFundedStartups(deals, selectedCountryInsight);

    container.innerHTML = `
        <div class="insight-card country-selector">
            <label for="country-intel-select"><strong>Country:</strong></label>
            <select id="country-intel-select">
                ${countries.map(country => `<option value="${escapeHtml(country.name)}" ${country.name === selectedCountryInsight ? 'selected' : ''}>${escapeHtml(country.name)}</option>`).join('')}
            </select>
        </div>

        <div class="insight-grid">
            <section class="insight-card span-12">
                <h2>${escapeHtml(selectedCountryInsight)} Intelligence</h2>
                <div class="kpi-grid">
                    ${buildKpiTile('Country Deals', countryDeals.length.toLocaleString())}
                    ${buildKpiTile('Country Funding', formatCurrencyCompact(countryFunding))}
                    ${buildKpiTile('Unique Investors', summarizeInvestors(countryDeals).length.toLocaleString())}
                    ${buildKpiTile('Actively Hiring', countryDeals.filter(d => d.HiringFilter === 'Yes').length.toLocaleString())}
                </div>
            </section>

            <section class="insight-card span-6">
                <h3>Funding Trend (Last 6 Months)</h3>
                <div class="trend-bars">${buildMonthlyFundingBars(monthlyTrend)}</div>
            </section>

            <section class="insight-card span-6">
                <h3>Round-Stage Mix</h3>
                ${roundMix.map(row => `<div class="mix-row"><span>${escapeHtml(row.name)}</span><div class="mix-bar" style="width:${row.pct}%;"></div><strong>${row.count}</strong></div>`).join('') || '<p>No round data.</p>'}
            </section>

            <section class="insight-card span-6">
                <h3>AI Sector Treemap</h3>
                <div class="treemap-grid treemap-grid-2col">${sectorMix.map(sector => `<div class="treemap-cell"><strong>${escapeHtml(sector.name)}</strong><br><span>${sector.count} deals</span></div>`).join('') || '<p>No sector data.</p>'}</div>
            </section>

            <section class="insight-card span-6">
                <h3>Top Startups</h3>
                <div class="insight-table-wrap" data-insight-table="country-top-startups">
                    <table class="simple-table sticky-header-table"><thead><tr>
                        ${insightSortableTh('country-top-startups', 'Startup', 0)}
                        ${insightSortableTh('country-top-startups', 'Round', 1)}
                        ${insightSortableTh('country-top-startups', 'Amount', 2, 'number')}
                    </tr></thead><tbody>
                        ${topStartups.map(deal => {
        const name = cleanString(deal.Startup_Name) || 'N/A';
        const round = cleanString(deal.RoundFilter) || 'N/A';
        const amt = Number.isFinite(deal.AmountValue) ? deal.AmountValue : '';
        return `<tr ${insightRowSortAttrs([name, round, amt])}><td>${escapeHtml(name)}</td><td>${escapeHtml(round)}</td><td>${escapeHtml(formatDealAmountForTable(deal))}</td></tr>`;
    }).join('') || '<tr><td colspan="3">No startups found</td></tr>'}
                    </tbody></table>
                </div>
            </section>

            <section class="insight-card span-12">
                <h3>Deal Velocity — Day-over-Day, Week-over-Week &amp; Month-over-Month</h3>
                <p class="insight-sub">% change in deal count and funding for <strong>${escapeHtml(selectedCountryInsight)}</strong> vs prior period.</p>
                ${buildDealVelocityPanel(velocity)}
            </section>

            <section class="insight-card span-12">
                <h3>Sector Velocity (WoW Deal Count)</h3>
                <p class="insight-sub">Week-over-week change in deal count per AI sector.</p>
                ${buildSectorVelocityTable(sectorVelocity)}
            </section>

            <section class="insight-card span-12">
                <h3>🆕 First-Time Funded Startups This Week — ${escapeHtml(selectedCountryInsight)}</h3>
                <p class="insight-sub">Startups appearing for the first time in the dataset within the most recent 7-day window.</p>
                ${buildFirstTimeFundedTable(firstTimers)}
            </section>
        </div>
    `;

    const selector = document.getElementById('country-intel-select');
    if (selector) {
        selector.addEventListener('change', event => {
            selectedCountryInsight = event.target.value;
            syncGlobalCountrySelector(countries);
            updateCountryContextSubtitle(selectedCountryInsight);
            renderCountryIntelligencePage(filteredDeals);
        });
    }

    const globalSelector = document.getElementById('country-intel-select-global');
    if (globalSelector) {
        globalSelector.value = selectedCountryInsight;
    }

    syncGlobalCountrySelector(countries);
    updateCountryContextSubtitle(selectedCountryInsight);
    applyStoredInsightSorts(container);
}

// ─── Deal Velocity (DoD / WoW / MoM) ─────────────────────────────────────────

function calcPctChange(curr, prev) {
    return prev === 0 ? (curr > 0 ? 100 : 0) : Math.round(((curr - prev) / prev) * 100);
}

function computeDealVelocity(deals, country) {
    const refDate = getReferenceDate(deals);
    const now = refDate.getTime();
    const dayMs = 24 * 60 * 60 * 1000;

    const thisWeekStart = now - 7 * dayMs;
    const lastWeekStart = now - 14 * dayMs;
    const thisMonthStart = now - 30 * dayMs;
    const lastMonthStart = now - 60 * dayMs;
    const thisDayStart = now - dayMs;
    const lastDayStart = now - 2 * dayMs;

    const scope = country ? deals.filter(d => d.Nation === country) : deals;

    function filterWindow(d, start, end) {
        return d.filter(x => Number.isFinite(x.DateValue) && x.DateValue >= start && x.DateValue < end);
    }

    const thisWeek = filterWindow(scope, thisWeekStart, now);
    const lastWeek = filterWindow(scope, lastWeekStart, thisWeekStart);
    const thisMonth = filterWindow(scope, thisMonthStart, now);
    const lastMonth = filterWindow(scope, lastMonthStart, thisMonthStart);
    const thisDay = filterWindow(scope, thisDayStart, now);
    const lastDay = filterWindow(scope, lastDayStart, thisDayStart);

    function funding(arr) { return arr.filter(d => Number.isFinite(d.AmountValue)).reduce((s, d) => s + d.AmountValue, 0); }

    return {
        dod: {
            dealCountChange: calcPctChange(thisDay.length, lastDay.length),
            fundingChange: calcPctChange(funding(thisDay), funding(lastDay)),
            thisDayDeals: thisDay.length,
            lastDayDeals: lastDay.length,
            thisDayFunding: funding(thisDay),
            lastDayFunding: funding(lastDay)
        },
        wow: {
            dealCountChange: calcPctChange(thisWeek.length, lastWeek.length),
            fundingChange: calcPctChange(funding(thisWeek), funding(lastWeek)),
            thisWeekDeals: thisWeek.length,
            lastWeekDeals: lastWeek.length,
            thisWeekFunding: funding(thisWeek),
            lastWeekFunding: funding(lastWeek)
        },
        mom: {
            dealCountChange: calcPctChange(thisMonth.length, lastMonth.length),
            fundingChange: calcPctChange(funding(thisMonth), funding(lastMonth)),
            thisMonthDeals: thisMonth.length,
            lastMonthDeals: lastMonth.length,
            thisMonthFunding: funding(thisMonth),
            lastMonthFunding: funding(lastMonth)
        }
    };
}

function buildDealVelocityPanel(v) {
    function badge(pct) {
        const cls = pct > 0 ? 'velocity-up' : (pct < 0 ? 'velocity-down' : 'velocity-flat');
        const arrow = pct > 0 ? '▲' : (pct < 0 ? '▼' : '—');
        return `<span class="velocity-badge ${escapeHtml(cls)}">${arrow} ${pct > 0 ? '+' : ''}${pct}%</span>`;
    }
    return `
        <div class="velocity-grid velocity-grid-3">
            <div class="velocity-card">
                <div class="velocity-period">Day-over-Day</div>
                <div class="velocity-metric">
                    <span class="velocity-label">Deal Count</span>
                    <span class="velocity-nums">${v.dod.thisDayDeals} <em>vs</em> ${v.dod.lastDayDeals}</span>
                    ${badge(v.dod.dealCountChange)}
                </div>
                <div class="velocity-metric">
                    <span class="velocity-label">Funding</span>
                    <span class="velocity-nums">${formatCurrencyCompact(v.dod.thisDayFunding)} <em>vs</em> ${formatCurrencyCompact(v.dod.lastDayFunding)}</span>
                    ${badge(v.dod.fundingChange)}
                </div>
            </div>
            <div class="velocity-card">
                <div class="velocity-period">Week-over-Week</div>
                <div class="velocity-metric">
                    <span class="velocity-label">Deal Count</span>
                    <span class="velocity-nums">${v.wow.thisWeekDeals} <em>vs</em> ${v.wow.lastWeekDeals}</span>
                    ${badge(v.wow.dealCountChange)}
                </div>
                <div class="velocity-metric">
                    <span class="velocity-label">Funding</span>
                    <span class="velocity-nums">${formatCurrencyCompact(v.wow.thisWeekFunding)} <em>vs</em> ${formatCurrencyCompact(v.wow.lastWeekFunding)}</span>
                    ${badge(v.wow.fundingChange)}
                </div>
            </div>
            <div class="velocity-card">
                <div class="velocity-period">Month-over-Month</div>
                <div class="velocity-metric">
                    <span class="velocity-label">Deal Count</span>
                    <span class="velocity-nums">${v.mom.thisMonthDeals} <em>vs</em> ${v.mom.lastMonthDeals}</span>
                    ${badge(v.mom.dealCountChange)}
                </div>
                <div class="velocity-metric">
                    <span class="velocity-label">Funding</span>
                    <span class="velocity-nums">${formatCurrencyCompact(v.mom.thisMonthFunding)} <em>vs</em> ${formatCurrencyCompact(v.mom.lastMonthFunding)}</span>
                    ${badge(v.mom.fundingChange)}
                </div>
            </div>
        </div>`;
}

function computeSectorVelocity(countryDeals) {
    const refDate = getReferenceDate(countryDeals.length ? countryDeals : []);
    const now = refDate.getTime();
    const dayMs = 24 * 60 * 60 * 1000;
    const thisWeekStart = now - 7 * dayMs;
    const lastWeekStart = now - 14 * dayMs;

    const sectors = ['GenAI', 'Infrastructure', 'Healthcare AI', 'Enterprise AI', 'Robotics', 'Fintech AI', 'Security AI', 'Climate AI', 'Other AI'];
    return sectors.map(sector => {
        const thisWeek = countryDeals.filter(d => Number.isFinite(d.DateValue) && d.DateValue >= thisWeekStart && classifyDealSector(d) === sector).length;
        const lastWeek = countryDeals.filter(d => Number.isFinite(d.DateValue) && d.DateValue >= lastWeekStart && d.DateValue < thisWeekStart && classifyDealSector(d) === sector).length;
        const pct = calcPctChange(thisWeek, lastWeek);
        return { sector, thisWeek, lastWeek, pct };
    }).filter(r => r.thisWeek > 0 || r.lastWeek > 0);
}

function buildSectorVelocityTable(rows) {
    if (!rows.length) return '<p>No sector data for selected country.</p>';
    const tableRows = rows.map(r => {
        const cls = r.pct > 0 ? 'velocity-up' : (r.pct < 0 ? 'velocity-down' : 'velocity-flat');
        const arrow = r.pct > 0 ? '▲' : (r.pct < 0 ? '▼' : '—');
        return `<tr ${insightRowSortAttrs([r.sector, r.thisWeek, r.lastWeek, r.pct])}>
            <td>${escapeHtml(r.sector)}</td>
            <td>${r.thisWeek}</td>
            <td>${r.lastWeek}</td>
            <td><span class="velocity-badge ${escapeHtml(cls)}">${arrow} ${r.pct > 0 ? '+' : ''}${r.pct}%</span></td>
        </tr>`;
    }).join('');
    return `
        <div class="insight-table-wrap insight-table-wrap--wide" data-insight-table="sector-velocity">
            <table class="simple-table sticky-header-table">
                <thead><tr>
                    ${insightSortableTh('sector-velocity', 'Sector', 0)}
                    ${insightSortableTh('sector-velocity', 'This Week', 1, 'number')}
                    ${insightSortableTh('sector-velocity', 'Last Week', 2, 'number')}
                    ${insightSortableTh('sector-velocity', 'WoW Change', 3, 'number')}
                </tr></thead>
                <tbody>${tableRows}</tbody>
            </table>
        </div>
    `;
}

// ─── Heat Signals (4-week rolling z-score) ───────────────────────────────────

function computeCountryHeatSignals(deals) {
    const refDate = getReferenceDate(deals);
    const now = refDate.getTime();
    const weekMs = 7 * 24 * 60 * 60 * 1000;

    const countryNames = [...new Set(deals.map(d => d.Nation).filter(Boolean))];

    return countryNames.map(country => {
        const countryDeals = deals.filter(d => d.Nation === country);
        const weekCounts = [];
        for (let i = 0; i < 8; i++) {
            const weekStart = now - (i + 1) * weekMs;
            const weekEnd = now - i * weekMs;
            weekCounts.unshift(countryDeals.filter(d => Number.isFinite(d.DateValue) && d.DateValue >= weekStart && d.DateValue < weekEnd).length);
        }
        const lastWeekCount = weekCounts[weekCounts.length - 1];
        const baseline = weekCounts.slice(0, 7);
        const mean = baseline.reduce((s, v) => s + v, 0) / (baseline.length || 1);
        const variance = baseline.reduce((s, v) => s + (v - mean) ** 2, 0) / (baseline.length || 1);
        const stdDev = Math.sqrt(variance);
        const zScore = stdDev === 0 ? 0 : Number(((lastWeekCount - mean) / stdDev).toFixed(2));
        const signal = zScore > 1 ? 'heating-up' : (zScore < -1 ? 'cooling-down' : 'stable');
        const totalDeals = countryDeals.length;
        return { country, zScore, signal, lastWeekCount, mean: parseFloat(mean.toFixed(1)), totalDeals };
    })
        .filter(r => r.totalDeals >= 3)
        .sort((a, b) => b.zScore - a.zScore);
}

function buildHeatSignalsTable(signals) {
    if (!signals.length) return '<p>Not enough data for heat signal analysis.</p>';
    const rows = signals.map(s => {
        const icon = s.signal === 'heating-up' ? '🔥' : (s.signal === 'cooling-down' ? '🧊' : '➖');
        const cls = s.signal === 'heating-up' ? 'heat-up' : (s.signal === 'cooling-down' ? 'heat-down' : 'heat-stable');
        const signalLabel = s.signal.replace('-', ' ');
        return `<tr ${insightRowSortAttrs([s.country, signalLabel, s.zScore, s.lastWeekCount, s.mean])}>
            <td>${escapeHtml(s.country)}</td>
            <td><span class="heat-badge ${escapeHtml(cls)}">${icon} ${escapeHtml(signalLabel)}</span></td>
            <td>${s.zScore > 0 ? '+' : ''}${s.zScore}</td>
            <td>${s.lastWeekCount}</td>
            <td>${s.mean}</td>
        </tr>`;
    }).join('');
    return `<div class="table-scroll-wrap" data-insight-table="heat-signals"><table class="simple-table sticky-header-table"><thead><tr>
        ${insightSortableTh('heat-signals', 'Country', 0)}
        ${insightSortableTh('heat-signals', 'Signal', 1)}
        ${insightSortableTh('heat-signals', 'Z-Score', 2, 'number')}
        ${insightSortableTh('heat-signals', 'Last Week', 3, 'number')}
        ${insightSortableTh('heat-signals', '8-Wk Avg', 4, 'number')}
    </tr></thead><tbody>${rows}</tbody></table></div>`;
}

// ─── First-Time Funded Startups ──────────────────────────────────────────────

function findFirstTimeFundedStartups(deals, country) {
    const refDate = getReferenceDate(deals);
    const now = refDate.getTime();
    const weekMs = 7 * 24 * 60 * 60 * 1000;
    const thisWeekStart = now - weekMs;

    const scope = country ? deals.filter(d => d.Nation === country) : deals;

    const earliestByStartup = new Map();
    scope.forEach(deal => {
        const name = cleanString(deal.Startup_Name).toLowerCase();
        if (!name) return;
        if (!earliestByStartup.has(name) || (Number.isFinite(deal.DateValue) && deal.DateValue < earliestByStartup.get(name).DateValue)) {
            earliestByStartup.set(name, deal);
        }
    });

    return [...earliestByStartup.values()]
        .filter(deal => Number.isFinite(deal.DateValue) && deal.DateValue >= thisWeekStart)
        .sort((a, b) => b.DateValue - a.DateValue)
        .slice(0, 20);
}

function buildFirstTimeFundedTable(startups) {
    if (!startups.length) return '<p>No first-time funded startups found this week for the selected country.</p>';
    const rows = startups.map(d => {
        const name = cleanString(d.Startup_Name) || 'N/A';
        const round = cleanString(d.RoundFilter) || 'N/A';
        const amountStr = formatDealAmountForTable(d);
        const dateStr = formatDate(d.Date_Captured);
        const descShort = (cleanString(d.Description) || 'N/A').slice(0, 100);
        const descSort = cleanString(d.Description) || 'N/A';
        const ts = Number.isFinite(d.DateValue) ? d.DateValue : '';
        const amountSort = Number.isFinite(d.AmountValue) ? d.AmountValue : '';
        return `<tr ${insightRowSortAttrs([name, round, amountSort, ts, descSort])}>
        <td><strong>${escapeHtml(name)}</strong></td>
        <td>${escapeHtml(round)}</td>
        <td>${escapeHtml(amountStr)}</td>
        <td>${escapeHtml(dateStr)}</td>
        <td class="description-cell">${escapeHtml(descShort)}${(cleanString(d.Description) || '').length > 100 ? '…' : ''}</td>
    </tr>`;
    }).join('');
    return `
        <div class="insight-table-wrap insight-table-wrap--wide" data-insight-table="first-time">
            <table class="simple-table sticky-header-table">
                <thead><tr>
                    ${insightSortableTh('first-time', 'Startup', 0)}
                    ${insightSortableTh('first-time', 'Round', 1)}
                    ${insightSortableTh('first-time', 'Amount', 2, 'number')}
                    ${insightSortableTh('first-time', 'Date', 3, 'number')}
                    ${insightSortableTh('first-time', 'Description', 4)}
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
    `;
}

function renderInvestorStartupPage(deals) {
    const container = document.getElementById('investor-intelligence-content');
    if (!container) return;

    const investors = summarizeInvestors(deals).slice(0, 20);
    const countries = summarizeCountries(deals).slice(0, 10).map(row => row.name);
    const startups = summarizeStartupFunding(deals).slice(0, 25);
    const recentDeals = [...deals]
        .filter(d => Number.isFinite(d.DateValue))
        .sort((a, b) => b.DateValue - a.DateValue)
        .slice(0, 12);

    container.innerHTML = `
        <div class="insight-grid">
            <section class="insight-card span-5">
                <h3>Top Investors</h3>
                <div class="insight-table-wrap" data-insight-table="top-investors">
                    <table class="simple-table sticky-header-table"><thead><tr>
                        ${insightSortableTh('top-investors', 'Investor', 0)}
                        ${insightSortableTh('top-investors', 'Deals', 1, 'number')}
                    </tr></thead><tbody>
                        ${investors.map(inv => `<tr ${insightRowSortAttrs([inv.name, inv.count])}><td>${escapeHtml(inv.name)}</td><td>${inv.count}</td></tr>`).join('') || '<tr><td colspan="2">No investor data</td></tr>'}
                    </tbody></table>
                </div>
            </section>

            <section class="insight-card span-7">
                <h3>Investor-Country Heat Map</h3>
                ${buildInvestorCountryHeatmap(investors.slice(0, 10), countries, deals)}
            </section>

            <section class="insight-card span-7">
                <h3>Startup Momentum Matrix</h3>
                <p class="insight-sub">Ranks startups by cumulative funding and repeat deal momentum.</p>
                ${buildStartupMomentumMatrix(startups)}
            </section>

            <section class="insight-card span-5">
                <h3>Recent Deal Feed</h3>
                <div class="feed-list feed-scroll">
                    ${recentDeals.map(deal => `<div class="feed-item"><strong>${escapeHtml(cleanString(deal.Startup_Name) || 'N/A')}</strong><br><span>${escapeHtml(deal.Nation || 'Unknown')} • ${escapeHtml(cleanString(deal.RoundFilter) || 'Round N/A')} • ${escapeHtml(formatDealAmountForTable(deal))}</span><br><small>${escapeHtml(formatDate(deal.Date_Captured))}</small></div>`).join('') || '<p>No recent deals available.</p>'}
                </div>
            </section>
        </div>
    `;

    applyStoredInsightSorts(container);
}

function buildKpiTile(label, value) {
    return `<article class="kpi-tile"><span class="kpi-label">${escapeHtml(label)}</span><span class="kpi-value">${escapeHtml(value)}</span></article>`;
}

function summarizeCountries(deals) {
    const map = new Map();
    deals.forEach(deal => {
        const name = cleanString(deal.Nation) || UNKNOWN_LABEL;
        if (!map.has(name)) map.set(name, { name, deals: 0, funding: 0 });
        const row = map.get(name);
        row.deals += 1;
        if (Number.isFinite(deal.AmountValue)) row.funding += deal.AmountValue;
    });
    return [...map.values()].sort((a, b) => (b.deals - a.deals) || (b.funding - a.funding));
}

function summarizeInvestors(deals) {
    const map = new Map();
    deals.forEach(deal => {
        splitInvestors(deal.Investors).forEach(name => {
            const normalized = normalizeInvestorName(name);
            if (!normalized) return;
            const display = normalizeInvestorDisplayName(normalized);
            if (!map.has(normalized)) {
                map.set(normalized, { name: display, count: 0 });
            }
            map.get(normalized).count += 1;
        });
    });
    return [...map.values()].sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
}

function splitInvestors(value) {
    return cleanString(value)
        .split(/,|;|\|| and /gi)
        .map(v => cleanString(v))
        .filter(v => v && !['n/a', 'na', 'unknown'].includes(v.toLowerCase()));
}

function summarizeDailyDeals(deals, windowDays) {
    const refDate = getReferenceDate(deals);
    const rows = [];
    for (let i = windowDays - 1; i >= 0; i -= 1) {
        const day = new Date(refDate.getTime() - (i * 24 * 60 * 60 * 1000));
        const start = startOfUtcDay(day).getTime();
        const end = endOfUtcDay(day).getTime();
        rows.push({
            label: `${day.getUTCMonth() + 1}/${day.getUTCDate()}`,
            count: deals.filter(d => Number.isFinite(d.DateValue) && d.DateValue >= start && d.DateValue <= end).length
        });
    }
    return rows;
}

function buildDailyTrendBars(rows) {
    const max = Math.max(...rows.map(r => r.count), 1);
    return rows.map(row => {
        const height = Math.max(6, (row.count / max) * 100);
        return `<div class="trend-bar" style="height:${height}%;" title="${escapeHtml(row.label)}: ${row.count} deals"><span>${escapeHtml(row.label)}</span></div>`;
    }).join('');
}

function normalizeInvestorName(value) {
    const normalized = cleanString(value)
        .toLowerCase()
        .replace(/[^\w\s]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    if (!normalized || ['n a', 'na', 'unknown', 'not disclosed', 'undisclosed', '1 more', 'existing investors', 'angel investors'].includes(normalized)) {
        return 'undisclosed';
    }
    return normalized;
}

function normalizeInvestorDisplayName(value) {
    if (value === 'undisclosed') {
        return 'Undisclosed';
    }
    if (value === 'nvidia') {
        return 'NVIDIA';
    }
    return value.split(' ').map(token => token ? token[0].toUpperCase() + token.slice(1) : token).join(' ');
}

function summarizeMonthlyFunding(deals, months) {
    const refDate = getReferenceDate(deals);
    const output = [];
    for (let i = months - 1; i >= 0; i -= 1) {
        const dt = new Date(Date.UTC(refDate.getUTCFullYear(), refDate.getUTCMonth() - i, 1));
        const y = dt.getUTCFullYear();
        const m = dt.getUTCMonth();
        const amount = deals
            .filter(d => Number.isFinite(d.DateValue) && new Date(d.DateValue).getUTCFullYear() === y && new Date(d.DateValue).getUTCMonth() === m)
            .reduce((sum, d) => sum + (Number.isFinite(d.AmountValue) ? d.AmountValue : 0), 0);
        output.push({ label: `${m + 1}/${String(y).slice(-2)}`, amount });
    }
    return output;
}

function buildMonthlyFundingBars(rows) {
    const max = Math.max(...rows.map(r => r.amount), 1);
    return rows.map(row => {
        const height = Math.max(8, (row.amount / max) * 100);
        return `<div class="trend-bar" style="height:${height}%;" title="${escapeHtml(row.label)}: ${formatCurrencyCompact(row.amount)}"><span>${escapeHtml(row.label)}</span></div>`;
    }).join('');
}

function summarizeRoundMix(deals) {
    const map = new Map();
    deals.forEach(d => {
        const k = cleanString(d.RoundFilter) || UNKNOWN_LABEL;
        map.set(k, (map.get(k) || 0) + 1);
    });
    const total = deals.length || 1;
    return [...map.entries()]
        .map(([name, count]) => ({ name, count, pct: Math.max(6, Math.round((count / total) * 100)) }))
        .sort((a, b) => b.count - a.count);
}

function summarizeSectors(deals) {
    const keywords = {
        'GenAI': /genai|llm|foundation model|chatbot|language model/i,
        'Infrastructure': /infrastructure|compute|cloud|gpu|chip|semiconductor/i,
        'Healthcare AI': /health|biotech|clinical|medtech/i,
        'Enterprise AI': /enterprise|workflow|saas|automation/i,
        'Robotics': /robot|autonomous|drone/i,
        'Fintech AI': /fintech|bank|payments|insurance/i,
        'Security AI': /security|cyber/i,
        'Climate AI': /climate|energy|sustainab/i,
        'Other AI': /./
    };

    const map = new Map(Object.keys(keywords).map(k => [k, 0]));
    deals.forEach(deal => {
        const text = `${cleanString(deal.Description)} ${cleanString(deal.Startup_Name)}`;
        const hit = Object.entries(keywords).find(([name, pattern]) => name !== 'Other AI' && pattern.test(text));
        map.set(hit ? hit[0] : 'Other AI', (map.get(hit ? hit[0] : 'Other AI') || 0) + 1);
    });

    return [...map.entries()]
        .map(([name, count]) => ({ name, count }))
        .filter(item => item.count > 0)
        .sort((a, b) => b.count - a.count);
}

function summarizeStartupFunding(deals) {
    const map = new Map();
    deals.forEach(deal => {
        const key = cleanString(deal.Startup_Name);
        if (!key) return;
        if (!map.has(key)) map.set(key, { name: key, funding: 0, latest: 0, deals: 0 });
        const row = map.get(key);
        row.deals += 1;
        if (Number.isFinite(deal.AmountValue)) row.funding += deal.AmountValue;
        if (Number.isFinite(deal.DateValue)) row.latest = Math.max(row.latest, deal.DateValue);
    });
    return [...map.values()].sort((a, b) => b.funding - a.funding);
}

function buildInvestorCountryHeatmap(investors, countries, deals) {
    const matrix = investors.map(inv => {
        const lower = inv.name.toLowerCase();
        return countries.map(country => deals.filter(deal => deal.Nation === country && cleanString(deal.Investors).toLowerCase().includes(lower)).length);
    });
    const max = Math.max(1, ...matrix.flat());

    const headerCells = [
        insightSortableTh('inv-country-heat', 'Investor', 0),
        ...countries.map((c, ci) => insightSortableTh('inv-country-heat', c, ci + 1, 'number'))
    ].join('');
    const header = `<tr>${headerCells}</tr>`;
    const rows = investors.map((inv, idx) => {
        const cols = matrix[idx].map(value => {
            const alpha = value ? Math.min(0.85, value / max) : 0.08;
            return `<td><div class="heatmap-cell" style="background: rgba(37,99,235,${alpha});">${value}</div></td>`;
        }).join('');
        const sortVals = [inv.name, ...matrix[idx]];
        return `<tr ${insightRowSortAttrs(sortVals)}><td>${escapeHtml(inv.name)}</td>${cols}</tr>`;
    }).join('');

    return `
        <div class="insight-table-wrap insight-table-wrap--wide" data-insight-table="inv-country-heat">
            <table class="simple-table sticky-header-table investor-heatmap-table">
                <thead>${header}</thead>
                <tbody>${rows || '<tr><td colspan="2">No heatmap data</td></tr>'}</tbody>
            </table>
        </div>
    `;
}

function buildStartupMomentumMatrix(startups) {
    if (!startups.length) {
        return '<p>No startup momentum data available.</p>';
    }
    const rows = startups.map((startup, index) => {
        const latestDate = Number.isFinite(startup.latest) ? formatDate(startup.latest) : 'N/A';
        const rank = index + 1;
        return `<tr ${insightRowSortAttrs([rank, startup.name, startup.deals, startup.funding, startup.latest || ''])}>
            <td>${rank}</td>
            <td>${escapeHtml(startup.name)}</td>
            <td>${startup.deals.toLocaleString()}</td>
            <td>${formatCurrencyCompact(startup.funding)}</td>
            <td>${escapeHtml(latestDate)}</td>
        </tr>`;
    }).join('');
    return `
        <div class="bubble-table-wrap bubble-table-wrap--momentum" data-insight-table="startup-momentum">
            <table class="simple-table sticky-header-table startup-momentum-table">
                <thead><tr>
                    ${insightSortableTh('startup-momentum', '#', 0, 'number')}
                    ${insightSortableTh('startup-momentum', 'Startup', 1)}
                    ${insightSortableTh('startup-momentum', 'Deal Count', 2, 'number')}
                    ${insightSortableTh('startup-momentum', 'Total Funding', 3, 'number')}
                    ${insightSortableTh('startup-momentum', 'Last Seen', 4, 'number')}
                </tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
        <p class="chart-note">This view prioritizes interpretability: you can immediately compare startup funding scale, repeat funding cadence, and recency.</p>
    `;
}

const COUNTRY_COORDS = {
    'USA': [37.09, -95.71],
    'United States': [37.09, -95.71],
    'Canada': [56.13, -106.35],
    'Mexico': [23.63, -102.55],
    'Britain': [55.38, -3.44],
    'United Kingdom': [55.38, -3.44],
    'Germany': [51.17, 10.45],
    'France': [46.23, 2.21],
    'Netherlands': [52.13, 5.29],
    'Sweden': [60.13, 18.64],
    'Switzerland': [46.82, 8.23],
    'Spain': [40.46, -3.75],
    'Italy': [41.87, 12.57],
    'Denmark': [56.26, 9.50],
    'Finland': [61.92, 25.75],
    'Norway': [60.47, 8.47],
    'Luxembourg': [49.82, 6.13],
    'Portugal': [39.40, -8.22],
    'Belgium': [50.50, 4.47],
    'Austria': [47.52, 14.55],
    'Poland': [51.92, 19.15],
    'Israel': [31.05, 34.85],
    'UAE': [23.42, 53.85],
    'Dubai': [25.20, 55.27],
    'Saudi Arabia': [23.89, 45.08],
    'Turkey': [38.96, 35.24],
    'MENA': [25.00, 45.00],
    'India': [20.59, 78.96],
    'Pakistan': [30.38, 69.35],
    'Bangladesh': [23.68, 90.36],
    'China': [35.86, 104.20],
    'Japan': [36.20, 138.25],
    'South Korea': [35.91, 127.77],
    'Taiwan': [23.70, 121.00],
    'Hong Kong': [22.32, 114.17],
    'Singapore': [1.35, 103.82],
    'Indonesia': [-0.79, 113.92],
    'Malaysia': [4.21, 101.98],
    'Vietnam': [14.06, 108.28],
    'Thailand': [15.87, 100.99],
    'Philippines': [12.88, 121.77],
    'Australia': [-25.27, 133.78],
    'New Zealand': [-40.90, 174.89],
    'Brazil': [-14.24, -51.93],
    'Argentina': [-38.42, -63.62],
    'Chile': [-35.68, -71.54],
    'Colombia': [4.57, -74.30],
    'Nigeria': [9.08, 8.68],
    'Kenya': [-0.02, 37.91],
    'South Africa': [-30.56, 22.94],
    'Egypt': [26.82, 30.80]
};

let leafletMapInstance = null;

function ensureLeafletLoaded(done) {
    if (typeof L !== 'undefined') {
        done();
        return;
    }
    const existing = document.querySelector('script[data-leaflet-fallback]');
    if (existing) {
        let attempts = 0;
        const wait = () => {
            if (typeof L !== 'undefined' || attempts > 200) {
                done();
                return;
            }
            attempts += 1;
            setTimeout(wait, 50);
        };
        wait();
        return;
    }
    const script = document.createElement('script');
    script.src = 'https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js';
    script.async = true;
    script.crossOrigin = 'anonymous';
    script.setAttribute('data-leaflet-fallback', '1');
    script.onload = () => done();
    script.onerror = () => done();
    document.body.appendChild(script);
}

function initLeafletWorldMap(allCountries) {
    const mapEl = document.getElementById('world-map-leaflet');
    if (!mapEl) return;

    const buildMap = () => {
        if (typeof L === 'undefined') {
            mapEl.innerHTML = '<p style="text-align:center;padding:40px;color:#888;">Map unavailable: the mapping library failed to load. Try a hard refresh; if it persists, a browser extension may be blocking the map script.</p>';
            return;
        }
        mountLeafletMap(mapEl, allCountries);
    };

    if (typeof L !== 'undefined') {
        buildMap();
    } else {
        ensureLeafletLoaded(buildMap);
    }
}

function mountLeafletMap(mapEl, allCountries) {

    if (leafletMapInstance) {
        leafletMapInstance.remove();
        leafletMapInstance = null;
    }

    const maxDeals = allCountries.length ? allCountries[0].deals : 1;

    const map = L.map('world-map-leaflet', {
        center: [20, 10],
        zoom: 2,
        minZoom: 1,
        maxZoom: 6,
        zoomControl: true,
        scrollWheelZoom: false
    });
    leafletMapInstance = map;

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
        maxZoom: 19
    }).addTo(map);

    allCountries.forEach(country => {
        const coords = COUNTRY_COORDS[country.name];
        if (!coords) return;

        const radius = 6 + Math.round((country.deals / maxDeals) * 18);
        const circle = L.circleMarker(coords, {
            radius,
            color: '#4c51bf',
            fillColor: '#667eea',
            fillOpacity: 0.8,
            weight: 2
        }).addTo(map);

        const detailHtml =
            `<strong>${escapeHtml(country.name)}</strong><br>` +
            `Deals: ${country.deals.toLocaleString()}<br>` +
            `Funding: ${formatCurrencyCompact(country.funding)}`;

        circle.bindPopup(detailHtml);
        circle.bindTooltip(detailHtml, {
            sticky: true,
            direction: 'auto',
            opacity: 0.95,
            className: 'map-marker-tooltip'
        });
    });
}
