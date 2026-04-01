// Configuration
const DATA_PATH = '../data/';
let allDeals = [];
let filteredDeals = [];
let nations = [];
let selectedNations = new Set();
let currentSort = { column: null, direction: 'asc' };

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadData();
    setupEventListeners();
});

// Setup event listeners
function setupEventListeners() {
    // Multi-select nation dropdown
    document.getElementById('nation-select-btn').addEventListener('click', toggleNationDropdown);
    document.getElementById('select-all-nations').addEventListener('click', selectAllNations);
    document.getElementById('clear-all-nations').addEventListener('click', clearAllNations);
    
    // Date range filters
    document.getElementById('date-from').addEventListener('change', filterDeals);
    document.getElementById('date-to').addEventListener('change', filterDeals);
    
    // Search
    document.getElementById('search').addEventListener('input', filterDeals);
    
    // Reset filters
    document.getElementById('reset-filters').addEventListener('click', resetFilters);
    
    // Close dropdown when clicking outside
    document.addEventListener('click', (e) => {
        const dropdown = document.getElementById('nation-dropdown');
        const button = document.getElementById('nation-select-btn');
        if (!dropdown.contains(e.target) && !button.contains(e.target)) {
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
                const response = await fetch(`${DATA_PATH}${nation}.json`);
                const data = await response.json();
                
                if (data.deals && Array.isArray(data.deals)) {
                    // Add nation metadata to each deal
                    data.deals.forEach(deal => {
                        deal.Nation = nation;
                        deal.Flag = deal.Flag || getCountryFlag(nation);
                        // Normalize date field
                        deal.Date = deal.Date_Captured || deal.Date || '';
                    });
                    
                    allDeals = allDeals.concat(data.deals);
                    console.log(`✅ Loaded ${data.deals.length} deals from ${nation}`);
                }
            } catch (error) {
                console.warn(`⚠️ Failed to load ${nation}:`, error);
            }
        }
        
        if (allDeals.length === 0) {
            showError('No deals found in the data files.');
            return;
        }
        
        console.log(`✅ Total deals loaded: ${allDeals.length}`);
        
        // Initialize all nations as selected
        selectedNations = new Set(nations);
        
        // Set default date range (last 90 days)
        setDefaultDateRange();
        
        // Initial display
        filteredDeals = [...allDeals];
        displayStats(filteredDeals);
        displayDeals(filteredDeals);
        populateNationFilter();
        await updateLastUpdated();
        
        showLoading(false);
    } catch (error) {
        console.error('❌ Error loading data:', error);
        showError(`Failed to load data: ${error.message}`);
        showLoading(false);
    }
}

// Detect available nation files
async function detectNationFiles() {
    const potentialNations = [
        'Britain', 'USA', 'China', 'Germany', 'France', 
        'India', 'Canada', 'Australia', 'Japan', 'Singapore',
        'Israel', 'South Korea', 'Netherlands', 'Sweden', 'Switzerland',
        'Brazil', 'Mexico', 'Spain', 'Italy', 'Poland'
    ];
    
    const availableNations = [];
    
    for (const nation of potentialNations) {
        try {
            const response = await fetch(`${DATA_PATH}${nation}.json`, { method: 'HEAD' });
            if (response.ok) {
                availableNations.push(nation);
            }
        } catch (error) {
            // File doesn't exist, skip silently
        }
    }
    
    return availableNations;
}

// Populate nation filter checkboxes
function populateNationFilter() {
    const container = document.getElementById('nation-checkboxes');
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
    dropdown.classList.toggle('hidden');
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
    
    document.getElementById('date-to').valueAsDate = today;
    document.getElementById('date-from').valueAsDate = ninetyDaysAgo;
}

// Filter deals based on all criteria
function filterDeals() {
    let filtered = [...allDeals];
    
    // Filter by nation
    if (selectedNations.size > 0 && selectedNations.size < nations.length) {
        filtered = filtered.filter(deal => selectedNations.has(deal.Nation));
    }
    
    // Filter by date range
    const dateFrom = document.getElementById('date-from').value;
    const dateTo = document.getElementById('date-to').value;
    
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
    const searchTerm = document.getElementById('search').value.toLowerCase().trim();
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
    document.getElementById('search').value = '';
    
    // Reset sort
    currentSort = { column: null, direction: 'asc' };
    
    // Refilter
    filterDeals();
}

// Display statistics
function displayStats(deals) {
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
    
    document.getElementById('stats-cards').innerHTML = statsHTML;
}

// Display deals in sortable table
function displayDeals(deals) {
    document.getElementById('deal-count').textContent = deals.length;
    
    if (deals.length === 0) {
        document.getElementById('deals-table').innerHTML = '<p class="no-results">No deals found matching your criteria.</p>';
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
    
    document.getElementById('deals-table').innerHTML = tableHTML;
}

// Sort deals by column
function sortDeals(column, toggle = true) {
    // Determine sort direction
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
        
        // Special handling for Amount
        if (column === 'Amount') {
            valA = parseFloat(valA.toString().replace(/[^0-9.]/g, '')) || 0;
            valB = parseFloat(valB.toString().replace(/[^0-9.]/g, '')) || 0;
            return (valA - valB) * direction;
        }
        
        // Special handling for Date
        if (column === 'Date') {
            valA = new Date(valA || '1970-01-01');
            valB = new Date(valB || '1970-01-01');
            return (valA - valB) * direction;
        }
        
        // String comparison
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
            document.getElementById('last-updated').textContent = `Last updated: ${lastUpdated.trim()}`;
            document.getElementById('sync-time').textContent = lastUpdated.trim();
        }
    } catch (error) {
        document.getElementById('sync-time').textContent = 'Unknown';
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
        'China': '🇨🇳',
        'Germany': '🇩🇪',
        'France': '🇫🇷',
        'India': '🇮🇳',
        'Canada': '🇨🇦',
        'Australia': '🇦🇺',
        'Japan': '🇯🇵',
        'Singapore': '🇸🇬',
        'Israel': '🇮🇱',
        'South Korea': '🇰🇷',
        'Netherlands': '🇳🇱',
        'Sweden': '🇸🇪',
        'Switzerland': '🇨🇭'
    };
    return flags[nation] || '🌍';
}

function showLoading(show) {
    const content = document.getElementById('dashboard-content');
    if (show) {
        content.innerHTML = '<div class="loading">⏳ Loading data...</div>';
    }
}

function showError(message) {
    const content = document.getElementById('dashboard-content');
    content.innerHTML = `<div class="error">❌ ${message}</div>`;
}
