// Configuration
const DATA_PATH = 'https://github.com/mohit-chhaparia/Global-VC-Intelligence-Dashboard/tree/main/data';
let allDeals = [];
let filteredDeals = [];
let nations = [];
let selectedNations = new Set();
let currentSort = { column: null, direction: 'asc' };

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    console.log('🚀 Dashboard initializing...');
    loadData();
});

// Setup event listeners after DOM is loaded
function setupEventListeners() {
    // Multi-select nation dropdown
    const nationBtn = document.getElementById('nation-select-btn');
    const selectAll = document.getElementById('select-all-nations');
    const clearAll = document.getElementById('clear-all-nations');
    
    if (nationBtn) nationBtn.addEventListener('click', toggleNationDropdown);
    if (selectAll) selectAll.addEventListener('click', selectAllNations);
    if (clearAll) clearAll.addEventListener('click', clearAllNations);
    
    // Date range filters
    const dateFrom = document.getElementById('date-from');
    const dateTo = document.getElementById('date-to');
    if (dateFrom) dateFrom.addEventListener('change', filterDeals);
    if (dateTo) dateTo.addEventListener('change', filterDeals);
    
    // Search
    const search = document.getElementById('search');
    if (search) search.addEventListener('input', filterDeals);
    
    // Reset filters
    const resetBtn = document.getElementById('reset-filters');
    if (resetBtn) resetBtn.addEventListener('click', resetFilters);
    
    // Close dropdown when clicking outside
    document.addEventListener('click', (e) => {
        const dropdown = document.getElementById('nation-dropdown');
        const button = document.getElementById('nation-select-btn');
        if (dropdown && button && !dropdown.contains(e.target) && !button.contains(e.target)) {
            dropdown.classList.add('hidden');
        }
    });
}

// Load data from JSON files
async function loadData() {
    try {
        showLoading(true);
        console.log('🔍 Starting data load...');
        
        // Detect available nations
        nations = await detectNationFiles();
        
        if (nations.length === 0) {
            showError('No data files found. Please check that JSON files exist in the data directory.');
            console.error('❌ No nation files detected');
            return;
        }
        
        console.log(`✅ Found ${nations.length} nations:`, nations);
        
        // Load data from each nation
        for (const nation of nations) {
            try {
                console.log(`📥 Loading: ${DATA_PATH}${nation}.json`);
                const response = await fetch(`${DATA_PATH}${nation}.json`);
                
                if (!response.ok) {
                    console.error(`❌ HTTP ${response.status} for ${nation}.json`);
                    continue;
                }
                
                const data = await response.json();
                console.log(`📊 ${nation} data structure:`, Object.keys(data));
                
                if (data.deals && Array.isArray(data.deals)) {
                    // Add nation metadata to each deal
                    data.deals.forEach(deal => {
                        deal.Nation = nation;
                        deal.Flag = getCountryFlag(nation);
                        deal.Date = deal.Date_Captured || deal.Date || '';
                    });
                    
                    allDeals = allDeals.concat(data.deals);
                    console.log(`✅ Loaded ${data.deals.length} deals from ${nation}`);
                } else {
                    console.warn(`⚠️ No deals array in ${nation}.json`);
                }
            } catch (error) {
                console.error(`⚠️ Failed to load ${nation}:`, error);
            }
        }
        
        if (allDeals.length === 0) {
            showError('No deals found in the data files. Please check the JSON structure.');
            console.error('❌ No deals loaded from any file');
            return;
        }
        
        console.log(`✅ Total deals loaded: ${allDeals.length}`);
        
        // Initialize all nations as selected
        selectedNations = new Set(nations);
        
        // Set default date range (last 90 days)
        setDefaultDateRange();
        
        // Setup event listeners now that we have data
        setupEventListeners();
        
        // Initial display
        filteredDeals = [...allDeals];
        displayStats(filteredDeals);
        displayDeals(filteredDeals);
        populateNationFilter();
        await updateLastUpdated();
        
        showLoading(false);
        console.log('✅ Dashboard loaded successfully!');
    } catch (error) {
        console.error('❌ Critical error loading data:', error);
        showError(`Failed to load data: ${error.message}`);
        showLoading(false);
    }
}

// Detect available nation files
async function detectNationFiles() {
    const potentialNations = [
        'Britain',
        'Canada',
        'Dubai_UAE',
        'India',
        'Israel',
        'MENA',
        'Singapore',
        'UAE',
        'USA'
    ];
    
    const availableNations = [];
    
    for (const nation of potentialNations) {
        try {
            const response = await fetch(`${DATA_PATH}${nation}.json`, { method: 'HEAD' });
            if (response.ok) {
                availableNations.push(nation);
            }
        } catch (error) {
            // File doesn't exist, skip
        }
    }
    
    return availableNations;
}

// Populate nation filter checkboxes
function populateNationFilter() {
    const container = document.getElementById('nation-checkboxes');
    if (!container) return;
    
    container.innerHTML = '';
    
    nations.sort().forEach(nation => {
        const label = document.createElement('label');
        label.className = 'checkbox-item';
        
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = nation;
        checkbox.checked = selectedNations.has(nation);
        checkbox.addEventListener('change', handleNationCheckbox);
        
        const span = document.createElement('span');
        span.textContent = `${getCountryFlag(nation)} ${nation}`;
        
        label.appendChild(checkbox);
        label.appendChild(span);
        container.appendChild(label);
    });
    
    updateNationButtonText();
}

// Handle nation checkbox change
function handleNationCheckbox(e) {
    const nation = e.target.value;
    if (e.target.checked) {
        selectedNations.add(nation);
    } else {
        selectedNations.delete(nation);
    }
    updateNationButtonText();
    filterDeals();
}

// Toggle nation dropdown
function toggleNationDropdown(e) {
    e.stopPropagation();
    const dropdown = document.getElementById('nation-dropdown');
    if (dropdown) {
        dropdown.classList.toggle('hidden');
    }
}

// Select all nations
function selectAllNations(e) {
    e.stopPropagation();
    selectedNations = new Set(nations);
    document.querySelectorAll('#nation-checkboxes input[type="checkbox"]').forEach(cb => {
        cb.checked = true;
    });
    updateNationButtonText();
    filterDeals();
}

// Clear all nations
function clearAllNations(e) {
    e.stopPropagation();
    selectedNations.clear();
    document.querySelectorAll('#nation-checkboxes input[type="checkbox"]').forEach(cb => {
        cb.checked = false;
    });
    updateNationButtonText();
    filterDeals();
}

// Update nation button text
function updateNationButtonText() {
    const button = document.getElementById('nation-select-btn');
    if (!button) return;
    
    const count = selectedNations.size;
    const total = nations.length;
    
    if (count === 0) {
        button.innerHTML = 'Select Nations <span class="arrow">▼</span>';
    } else if (count === total) {
        button.innerHTML = `All Nations (${total}) <span class="arrow">▼</span>`;
    } else {
        button.innerHTML = `${count} of ${total} Nations <span class="arrow">▼</span>`;
    }
}

// Set default date range
function setDefaultDateRange() {
    const today = new Date();
    const ninetyDaysAgo = new Date(today.getTime() - (90 * 24 * 60 * 60 * 1000));
    
    const dateFrom = document.getElementById('date-from');
    const dateTo = document.getElementById('date-to');
    
    if (dateTo) dateTo.valueAsDate = today;
    if (dateFrom) dateFrom.valueAsDate = ninetyDaysAgo;
}

// Filter deals based on all criteria
function filterDeals() {
    let filtered = [...allDeals];
    
    // Filter by nation
    if (selectedNations.size > 0 && selectedNations.size < nations.length) {
        filtered = filtered.filter(deal => selectedNations.has(deal.Nation));
    }
    
    // Filter by date range
    const dateFromInput = document.getElementById('date-from');
    const dateToInput = document.getElementById('date-to');
    const dateFrom = dateFromInput ? dateFromInput.value : '';
    const dateTo = dateToInput ? dateToInput.value : '';
    
    if (dateFrom || dateTo) {
        filtered = filtered.filter(deal => {
            if (!deal.Date) return true;
            const dealDate = new Date(deal.Date);
            
            if (dateFrom && dealDate < new Date(dateFrom)) return false;
            if (dateTo && dealDate > new Date(dateTo)) return false;
            return true;
        });
    }
    
    // Filter by search term
    const searchInput = document.getElementById('search');
    const searchTerm = searchInput ? searchInput.value.toLowerCase().trim() : '';
    
    if (searchTerm) {
        filtered = filtered.filter(deal => 
            (deal.Startup_Name || '').toLowerCase().includes(searchTerm) ||
            (deal.Description || '').toLowerCase().includes(searchTerm) ||
            (deal.Investors || '').toLowerCase().includes(searchTerm) ||
            (deal.Round || '').toLowerCase().includes(searchTerm) ||
            (deal.Founders || '').toLowerCase().includes(searchTerm)
        );
    }
    
    filteredDeals = filtered;
    
    // Reapply current sort if exists
    if (currentSort.column) {
        sortDeals(currentSort.column, false);
    } else {
        displayStats(filteredDeals);
        displayDeals(filteredDeals);
    }
}

// Reset all filters
function resetFilters() {
    // Reset nations
    selectedNations = new Set(nations);
    document.querySelectorAll('#nation-checkboxes input[type="checkbox"]').forEach(cb => {
        cb.checked = true;
    });
    updateNationButtonText();
    
    // Reset dates
    setDefaultDateRange();
    
    // Reset search
    const searchInput = document.getElementById('search');
    if (searchInput) searchInput.value = '';
    
    // Reset sort
    currentSort = { column: null, direction: 'asc' };
    
    // Refilter
    filterDeals();
}

// Display statistics
function displayStats(deals) {
    const statsContainer = document.getElementById('stats-cards');
    if (!statsContainer) return;
    
    const totalDeals = deals.length;
    const totalFunding = deals.reduce((sum, deal) => {
        const amount = parseFloat((deal.Amount || '0').replace(/[^0-9.]/g, ''));
        return sum + amount;
    }, 0);
    
    const uniqueCountries = new Set(deals.map(deal => deal.Nation)).size;
    const avgDealSize = totalDeals > 0 ? totalFunding / totalDeals : 0;
    
    const statsHTML = `
        <div class="stat-card">
            <div class="stat-icon">💼</div>
            <h3>${totalDeals.toLocaleString()}</h3>
            <p>Total Deals</p>
        </div>
        <div class="stat-card">
            <div class="stat-icon">💰</div>
            <h3>$${formatLargeNumber(totalFunding)}</h3>
            <p>Total Funding</p>
        </div>
        <div class="stat-card">
            <div class="stat-icon">🌍</div>
            <h3>${uniqueCountries}</h3>
            <p>Countries</p>
        </div>
        <div class="stat-card">
            <div class="stat-icon">📊</div>
            <h3>$${formatLargeNumber(avgDealSize)}</h3>
            <p>Avg Deal Size</p>
        </div>
    `;
    
    statsContainer.innerHTML = statsHTML;
}

// Display deals in sortable table
function displayDeals(deals) {
    const dealCount = document.getElementById('deal-count');
    const dealsTable = document.getElementById('deals-table');
    
    if (dealCount) dealCount.textContent = deals.length;
    if (!dealsTable) return;
    
    if (deals.length === 0) {
        dealsTable.innerHTML = '<p class="no-results">No deals found matching your criteria.</p>';
        return;
    }
    
    const tableHTML = `
        <div class="table-wrapper">
            <table>
                <thead>
                    <tr>
                        <th onclick="sortDeals('Nation')" class="sortable">
                            Nation <span class="sort-icon">${getSortIcon('Nation')}</span>
                        </th>
                        <th onclick="sortDeals('Startup_Name')" class="sortable">
                            Startup <span class="sort-icon">${getSortIcon('Startup_Name')}</span>
                        </th>
                        <th onclick="sortDeals('Description')" class="sortable">
                            Description <span class="sort-icon">${getSortIcon('Description')}</span>
                        </th>
                        <th onclick="sortDeals('Amount')" class="sortable">
                            Amount <span class="sort-icon">${getSortIcon('Amount')}</span>
                        </th>
                        <th onclick="sortDeals('Round')" class="sortable">
                            Round <span class="sort-icon">${getSortIcon('Round')}</span>
                        </th>
                        <th onclick="sortDeals('Investors')" class="sortable">
                            Investors <span class="sort-icon">${getSortIcon('Investors')}</span>
                        </th>
                        <th onclick="sortDeals('Date')" class="sortable">
                            Date <span class="sort-icon">${getSortIcon('Date')}</span>
                        </th>
                    </tr>
                </thead>
                <tbody>
                    ${deals.map(deal => `
                        <tr>
                            <td class="nation-cell">
                                <span class="flag">${deal.Flag}</span>
                                <span class="nation-name">${deal.Nation}</span>
                            </td>
                            <td class="startup-cell">
                                <strong>${deal.Startup_Name || 'N/A'}</strong>
                            </td>
                            <td class="description-cell">${truncateText(deal.Description, 100)}</td>
                            <td class="amount-cell"><strong>${deal.Amount || 'N/A'}</strong></td>
                            <td>${deal.Round || 'N/A'}</td>
                            <td class="investors-cell">${truncateText(deal.Investors, 50)}</td>
                            <td>${formatDate(deal.Date)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;
    
    dealsTable.innerHTML = tableHTML;
}

// Sort deals by column
function sortDeals(column, toggle = true) {
    // Make sortDeals globally accessible
    window.sortDeals = sortDeals;
    
    if (toggle) {
        if (currentSort.column === column) {
            currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
        } else {
            currentSort.column = column;
            currentSort.direction = 'asc';
        }
    }
    
    const direction = currentSort.direction === 'asc' ? 1 : -1;
    
    filteredDeals.sort((a, b) => {
        let valA = a[column] || '';
        let valB = b[column] || '';
        
        if (column === 'Amount') {
            valA = parseFloat(valA.toString().replace(/[^0-9.]/g, '')) || 0;
            valB = parseFloat(valB.toString().replace(/[^0-9.]/g, '')) || 0;
            return (valA - valB) * direction;
        }
        
        if (column === 'Date') {
            valA = new Date(valA || '1970-01-01');
            valB = new Date(valB || '1970-01-01');
            return (valA - valB) * direction;
        }
        
        return valA.toString().localeCompare(valB.toString()) * direction;
    });
    
    displayStats(filteredDeals);
    displayDeals(filteredDeals);
}

// Get sort icon for column
function getSortIcon(column) {
    if (currentSort.column !== column) return '⇅';
    return currentSort.direction === 'asc' ? '↑' : '↓';
}

// Update last updated timestamp
async function updateLastUpdated() {
    try {
        const response = await fetch(`${DATA_PATH}last_updated.txt`);
        if (response.ok) {
            const lastUpdated = await response.text();
            const elem = document.getElementById('last-updated');
            const syncElem = document.getElementById('sync-time');
            if (elem) elem.textContent = `Last updated: ${lastUpdated.trim()}`;
            if (syncElem) syncElem.textContent = lastUpdated.trim();
        }
    } catch (error) {
        console.log('Could not load last_updated.txt');
    }
}

// Utility functions
function formatLargeNumber(num) {
    if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
    if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
    if (num >= 1e3) return `${(num / 1e3).toFixed(2)}K`;
    return num.toFixed(2);
}

function truncateText(text, maxLength) {
    if (!text) return 'N/A';
    if (text.length <= maxLength) return text;
    return text.substring(0, maxLength) + '...';
}

function formatDate(dateString) {
    if (!dateString) return 'N/A';
    try {
        const date = new Date(dateString);
        return date.toLocaleDateString('en-US', { 
            year: 'numeric', 
            month: 'short', 
            day: 'numeric' 
        });
    } catch {
        return dateString;
    }
}

function getCountryFlag(nation) {
    const flags = {
        'Britain': '🇬🇧',
        'USA': '🇺🇸',
        'Canada': '🇨🇦',
        'India': '🇮🇳',
        'Israel': '🇮🇱',
        'Singapore': '🇸🇬',
        'UAE': '🇦🇪',
        'Dubai_UAE': '🇦🇪',
        'MENA': '🌍'
    };
    return flags[nation] || '🌍';
}

function showLoading(show) {
    const content = document.getElementById('dashboard-content');
    if (!content) return;
    
    if (show) {
        content.innerHTML = '<div class="loading">⏳ Loading data...</div>';
    }
}

function showError(message) {
    const content = document.getElementById('dashboard-content');
    if (!content) return;
    
    content.innerHTML = `<div class="error">❌ ${message}</div>`;
}

// Make sortDeals globally accessible for onclick handlers
window.sortDeals = sortDeals;
