const DATA_PATH = '../data/';
let allDeals = [];
let nations = [];

async function detectNationFiles() {
    // Based on your data, you have Britain.json [1]
    const potentialNations = [
        'Britain', 'USA', 'China', 'Germany', 'France', 
        'India', 'Canada', 'Australia', 'Japan', 'Singapore',
        'Israel', 'South Korea', 'Netherlands', 'Sweden', 'Switzerland'
    ];
    
    const availableNations = [];
    
    for (const nation of potentialNations) {
        try {
            const response = await fetch(`${DATA_PATH}${nation}.json`, {
                method: 'GET',
                headers: { 'Content-Type': 'application/json' }
            });
            if (response.ok) {
                availableNations.push(nation);
                console.log(`✅ Found: ${nation}.json`);
            }
        } catch (error) {
            console.log(`❌ Not found: ${nation}.json`);
        }
    }
    
    if (availableNations.length === 0) {
        console.error('No data files found!');
    }
    
    return availableNations;
}

async function loadData() {
    try {
        showLoading(true);
        
        nations = await detectNationFiles();
        
        if (nations.length === 0) {
            showError('No data files found. Please check back later.');
            return;
        }
        
        for (const nation of nations) {
            try {
                const response = await fetch(`${DATA_PATH}${nation}.json`);
                const data = await response.json();
                
                if (data.deals && Array.isArray(data.deals)) {
                    data.deals.forEach(deal => {
                        deal.Nation = nation;
                        deal.Flag = deal.Flag || getCountryFlag(nation);
                    });
                    
                    allDeals = allDeals.concat(data.deals);
                }
            } catch (error) {
                console.warn(`Failed to load data for ${nation}:`, error);
            }
        }
        
        allDeals.sort((a, b) => {
            const amountA = parseFloat(a.Amount.replace(/[^0-9.]/g, '') || 0);
            const amountB = parseFloat(b.Amount.replace(/[^0-9.]/g, '') || 0);
            return amountB - amountA;
        });
        
        displayStats();
        displayDeals(allDeals);
        populateFilters();
        await updateLastUpdated();
        
        showLoading(false);
    } catch (error) {
        console.error('Error loading data:', error);
        showError('Failed to load data. Please try again later.');
        showLoading(false);
    }
}

function displayStats() {
    const totalDeals = allDeals.length;
    const totalFunding = allDeals.reduce((sum, deal) => {
        const amount = parseFloat(deal.Amount.replace(/[^0-9.]/g, '') || 0);
        return sum + amount;
    }, 0);
    
    const uniqueCountries = new Set(allDeals.map(deal => deal.Nation)).size;
    
    const avgDealSize = totalFunding / totalDeals;
    
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

function displayDeals(deals) {
    if (deals.length === 0) {
        document.getElementById('deals-table').innerHTML = '<p class="no-results">No deals found matching your criteria.</p>';
        return;
    }
    
    const tableHTML = `
        <div class="table-wrapper">
            <table>
                <thead>
                    <tr>
                        <th>Nation</th>
                        <th>Startup</th>
                        <th>Description</th>
                        <th>Amount</th>
                        <th>Round</th>
                        <th>Investors</th>
                        <th>Date</th>
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
                                <strong>${deal.Startup_Name}</strong>
                            </td>
                            <td class="description-cell">${truncateText(deal.Description, 100)}</td>
                            <td class="amount-cell"><strong>${deal.Amount}</strong></td>
                            <td>${deal.Round || 'N/A'}</td>
                            <td class="investors-cell">${truncateText(deal.Investors, 50)}</td>
                            <td>${formatDate(deal.Date_Captured)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;
    
    document.getElementById('deals-table').innerHTML = tableHTML;
}

function populateFilters() {
    const select = document.getElementById('nation-filter');
    
    select.innerHTML = '<option value="">All Nations</option>';
    
    nations.sort().forEach(nation => {
        const option = document.createElement('option');
        option.value = nation;
        option.textContent = `${getCountryFlag(nation)} ${nation}`;
        select.appendChild(option);
    });
    
    select.addEventListener('change', filterDeals);
    document.getElementById('search').addEventListener('input', filterDeals);
}

function filterDeals() {
    const nationFilter = document.getElementById('nation-filter').value;
    const searchTerm = document.getElementById('search').value.toLowerCase().trim();
    
    let filtered = allDeals;
    
    if (nationFilter) {
        filtered = filtered.filter(deal => deal.Nation === nationFilter);
    }
    
    if (searchTerm) {
        filtered = filtered.filter(deal => 
            (deal.Startup_Name || '').toLowerCase().includes(searchTerm) ||
            (deal.Description || '').toLowerCase().includes(searchTerm) ||
            (deal.Investors || '').toLowerCase().includes(searchTerm) ||
            (deal.Round || '').toLowerCase().includes(searchTerm)
        );
    }
    
    displayDeals(filtered);
}

async function updateLastUpdated() {
    try {
        const response = await fetch(`${DATA_PATH}last_updated.txt`);
        if (response.ok) {
            const lastUpdated = await response.text();
            document.getElementById('last-updated').textContent = `Last updated: ${lastUpdated.trim()}`;
            document.getElementById('sync-time').textContent = lastUpdated.trim();
        } else {
            document.getElementById('sync-time').textContent = 'Unknown';
        }
    } catch (error) {
        document.getElementById('sync-time').textContent = 'Unknown';
    }
}

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
        'Singapore': '🇸🇬'
    };
    return flags[nation] || '🌍';
}

function showLoading(show) {
    const content = document.getElementById('dashboard-content');
    if (show) {
        content.innerHTML = '<div class="loading">Loading data...</div>';
    }
}

function showError(message) {
    const content = document.getElementById('dashboard-content');
    content.innerHTML = `<div class="error">${message}</div>`;
}

document.addEventListener('DOMContentLoaded', () => {
    loadData();
});
