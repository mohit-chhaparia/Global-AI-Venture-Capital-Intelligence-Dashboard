const MANIFEST_PATH = 'data/manifest.json';
const LAST_UPDATED_PATH = 'data/last_updated.txt';
const FX_RATES_PATH = 'data/fx_rates.json';
const UNKNOWN_LABEL = 'Unknown';
const AMOUNT_SLIDER_MAX = 1000;
const MAX_REALISTIC_DEAL_AMOUNT = 250e9;
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
let dashboardInitialized = false;
let currencyToUsdRate = { ...FALLBACK_CURRENCY_TO_USD_RATE };

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
        await loadFxRates();
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
    allFilterOptions.hiring = sortByPreferredOrder(uniqueValues(allDeals.map(deal => deal.HiringFilter)), ['Hiring', 'Not Hiring', UNKNOWN_LABEL]);
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

const MULTI_SELECT_KEYS = ['nation', 'round', 'tier', 'linkedin', 'hiring', 'careers'];

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
    applyCurrentSort();
    displayStats(filteredDeals);

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
        case 'hiring': return v => sortByPreferredOrder(v, ['Hiring', 'Not Hiring', UNKNOWN_LABEL]);
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

            <div class="period-stat-grid">
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
                <div class="period-stat">
                    <span>Avg Size</span>
                    <strong>${formatCurrencyCompact(metrics.avgDealSize)}</strong>
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

    const hiringCount = deals.filter(d => d.HiringFilter === 'Hiring').length;
    const notHiringCount = deals.filter(d => d.HiringFilter === 'Not Hiring').length;
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
    if (filter === 'Hiring') {
        return renderBadge('Hiring', 'positive');
    }
    if (filter === 'Not Hiring') {
        return renderBadge('Not Hiring', 'negative');
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
    } else if (/\bthousand\b/.test(normalized) || /(?<![a-z])k(?![a-z])/.test(normalized)) {
        numericValue *= 1e3;
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

    if (normalized.includes('not hiring') || normalized === 'no' || normalized === 'false') {
        return 'Not Hiring';
    }

    if (normalized.includes('hiring') || normalized === 'yes' || normalized === 'true' || normalized.includes('open role')) {
        return 'Hiring';
    }

    return toTitleCase(cleaned);
}

function getHiringBadgeClass(hiringValue) {
    if (hiringValue === 'Hiring') {
        return 'positive';
    }

    if (hiringValue === 'Not Hiring') {
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

window.sortDeals = sortDeals;
