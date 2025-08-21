// Global variables
let map;
let markers = [];
let allOffers = [];
let filteredOffers = [];

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    initializeMap();
    loadData();
    setupEventListeners();
});

// Initialize Leaflet map
function initializeMap() {
    // Kraków center coordinates
    const krakowCenter = [50.0647, 19.9450];
    
    map = L.map('map').setView(krakowCenter, 12);
    
    // Add OpenStreetMap tiles
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors'
    }).addTo(map);
}

// Load data from CSV file
function loadData() {
    Papa.parse('../data/oferty_geo.csv', {
        download: true,
        header: true,
        complete: function(results) {
            allOffers = results.data.filter(offer => 
                offer.lat && offer.lon && 
                !isNaN(parseFloat(offer.lat)) && 
                !isNaN(parseFloat(offer.lon))
            );
            
            // Convert numeric fields
            allOffers.forEach(offer => {
                offer.lat = parseFloat(offer.lat);
                offer.lon = parseFloat(offer.lon);
                offer.najem_pln = parseFloat(offer.najem_pln) || 0;
                offer.czynsz_adm_pln = parseFloat(offer.czynsz_adm_pln) || 0;
                offer.metraz_m2 = parseFloat(offer.metraz_m2) || null;
            });
            

            

            
            filteredOffers = [...allOffers];
            
            updateMap();
            populateDistrictFilter();
            updateTotalOffers();
        },
        error: function(error) {
            console.error('Error loading CSV:', error);
            alert('Błąd podczas ładowania danych. Sprawdź czy plik CSV istnieje.');
        }
    });
}

// Setup event listeners
function setupEventListeners() {
    // Filter buttons
    document.getElementById('apply-filters').addEventListener('click', applyFilters);
    document.getElementById('clear-filters').addEventListener('click', clearFilters);
    
    // Toggle filters button
    document.getElementById('toggle-filters').addEventListener('click', toggleFilters);
    document.getElementById('toggle-filters').title = 'Pokaż filtry';
    

    
    // Modal close
    document.querySelectorAll('.close').forEach(closeBtn => {
        closeBtn.addEventListener('click', function() {
            const modal = this.closest('.modal');
            modal.style.display = 'none';
        });
    });
    
    // Close modals when clicking outside
    document.querySelectorAll('.modal').forEach(modal => {
        modal.addEventListener('click', function(e) {
            if (e.target === this) {
                this.style.display = 'none';
            }
        });
    });
    
    // Enter key in price inputs
    document.getElementById('min-price').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') applyFilters();
    });
    document.getElementById('max-price').addEventListener('keypress', function(e) {
        if (e.key === 'Enter') applyFilters();
    });
}

// Update map with current offers
function updateMap() {
    // Clear existing markers
    markers.forEach(marker => map.removeLayer(marker));
    markers = [];
    
    // Add new markers
    const prices = filteredOffers.map(offer => offer.najem_pln + (offer.czynsz_adm_pln || 0)).filter(price => price > 0);
    
    // Debug price ranges
    if (prices.length > 0) {
        const realisticPrices = prices.filter(p => p <= 5000);
        const expensivePrices = prices.filter(p => p > 5000);
        console.log('Realistic prices (≤5k):', realisticPrices.length, 'offers, range:', 
                   realisticPrices.length > 0 ? `${Math.min(...realisticPrices)}-${Math.max(...realisticPrices)} PLN` : 'none');
        console.log('Expensive prices (>5k):', expensivePrices.length, 'offers');
    }
    
    filteredOffers.forEach(offer => {
        const totalCost = offer.najem_pln + (offer.czynsz_adm_pln || 0);
        const color = getPriceColor(totalCost, prices);
        
        // Create custom icon with price-based color
        const customIcon = L.divIcon({
            className: 'custom-marker',
            html: `<div style="background-color: ${color}; width: 20px; height: 20px; border-radius: 50%; border: 2px solid white; box-shadow: 0 2px 4px rgba(0,0,0,0.3);"></div>`,
            iconSize: [20, 20],
            iconAnchor: [10, 10]
        });
        
        const marker = L.marker([offer.lat, offer.lon], { icon: customIcon })
            .addTo(map)
            .bindPopup(createPopupContent(offer))
            .on('mouseover', function() {
                this.openPopup();
            })
            .on('mouseout', function() {
                this.closePopup();
            })
            .on('click', () => showOfferDetails(offer));
        
        markers.push(marker);
    });
}

// Extract area from title
function extractArea(title) {
    if (!title) return null;
    
    // Look for patterns like "41,60 m²", "75 m2", "24m2", "60 m2", etc.
    const patterns = [
        /(\d+[,\d]*)\s*m²/i,
        /(\d+[,\d]*)\s*m2/i,
        /(\d+[,\d]*)\s*mkw/i,
        /(\d+[,\d]*)\s*m\s*kw/i
    ];
    
    for (const pattern of patterns) {
        const match = title.match(pattern);
        if (match) {
            return match[1].replace(',', '.');
        }
    }
    
    return null;
}

// Generate color based on price (green = cheap, red = expensive, burgundy = expensive)
function getPriceColor(price, prices) {
    if (prices.length === 0) return '#00ff00';
    
    // Cap max price at 5000 PLN for gradient (student-friendly)
    const MAX_GRADIENT_PRICE = 5000;
    
    // Sort prices and filter realistic ones
    const realisticPrices = prices.filter(p => p <= MAX_GRADIENT_PRICE);
    
    if (realisticPrices.length === 0) return '#00ff00';
    
    // Use min and max from realistic prices
    const minPrice = Math.min(...realisticPrices);
    const maxPrice = Math.max(...realisticPrices);
    
    // If price is above 5k, return burgundy
    if (price > MAX_GRADIENT_PRICE) {
        return '#800020'; // Burgundy for expensive offers
    }
    
    // Clamp price to realistic range
    const clampedPrice = Math.max(minPrice, Math.min(maxPrice, price));
    
    // Calculate ratio within realistic range
    const ratio = (clampedPrice - minPrice) / (maxPrice - minPrice);
    
    // Invert ratio so that cheap = green, expensive = red
    const invertedRatio = 1 - ratio;
    
    // Interpolate: red (expensive) -> yellow -> green (cheap)
    // Proporcje: 60% czerwony, 30% żółty, 10% zielony
    if (invertedRatio <= 0.6) {
        // Red to Yellow (invertedRatio 0-0.6)
        const localRatio = invertedRatio / 0.6; // 0-1
        const r = 255;
        const g = Math.round(255 * localRatio);
        const b = 0;
        return `rgb(${r}, ${g}, ${b})`;
    } else if (invertedRatio <= 0.9) {
        // Yellow to Green (invertedRatio 0.6-0.9)
        const localRatio = (invertedRatio - 0.6) / 0.3; // 0-1
        const r = Math.round(255 * (1 - localRatio));
        const g = 255;
        const b = 0;
        return `rgb(${r}, ${g}, ${b})`;
    } else {
        // Pure Green (invertedRatio 0.9-1.0)
        return `rgb(0, 255, 0)`;
    }
}

// Create popup content for marker hover
function createPopupContent(offer) {
    const totalCost = offer.najem_pln + (offer.czynsz_adm_pln || 0);
    const area = parseFloat(offer.metraz_m2) || extractArea(offer.title);
    
    // Dynamiczne formatowanie w zależności od długości tekstu
    const priceText = `${totalCost} PLN/mies.`;
    const areaText = area ? ` | ${area}m²` : '';
    const fullText = priceText + areaText;
    
    // Jeśli tekst jest długi, dzielimy na dwie linie
    if (fullText.length > 25) {
        return `
            <div class="popup-price">${priceText}</div>
            ${area ? `<div class="popup-price">${area}m²</div>` : ''}
            <div class="popup-click">Kliknij aby zobaczyć szczegóły</div>
        `;
    }
    
    return `
        <div class="popup-price">${fullText}</div>
        <div class="popup-click">Click to see details</div>
    `;
}

// Show offer details in modal
function showOfferDetails(offer) {
    const modal = document.getElementById('offer-modal');
    const modalContent = document.getElementById('modal-content');
    
    const totalCost = offer.najem_pln + (offer.czynsz_adm_pln || 0);
    
    modalContent.innerHTML = `
        <div class="offer-details">
            <h2>${offer.title || 'Mieszkanie'}</h2>
            
            <div class="detail-row">
                <span class="label">Address:</span>
                <span class="value">${offer.ulica}</span>
            </div>
            
            <div class="detail-row">
                <span class="label">District:</span>
                <span class="value">${offer.dzielnica || 'No data'}</span>
            </div>
            
            ${parseFloat(offer.metraz_m2) ? `
            <div class="detail-row">
                <span class="label">Area:</span>
                <span class="value">${parseFloat(offer.metraz_m2)} m²</span>
            </div>
            ` : ''}
            
            <div class="detail-row">
                <span class="label">Rent Price:</span>
                <span class="value price">${offer.najem_pln} PLN/month</span>
            </div>
            
            ${offer.czynsz_adm_pln ? `
            <div class="detail-row">
                <span class="label">Administrative Fee:</span>
                <span class="value price">${offer.czynsz_adm_pln} PLN/month</span>
            </div>
            ` : ''}
            
            <div class="detail-row">
                <span class="label">Total Cost:</span>
                <span class="value price">${totalCost} PLN/month</span>
            </div>
            
            <a href="${offer.url}" target="_blank" class="link">
                View listing on Otodom →
            </a>
        </div>
    `;
    
    modal.style.display = 'block';
}

// Toggle filters sidebar
function toggleFilters() {
    const sidebar = document.getElementById('sidebar');
    const toggleBtn = document.getElementById('toggle-filters');
    
    if (sidebar.classList.contains('hidden')) {
        sidebar.classList.remove('hidden');
        toggleBtn.textContent = '✕';
        toggleBtn.title = 'Ukryj filtry';
    } else {
        sidebar.classList.add('hidden');
        toggleBtn.textContent = '☰';
        toggleBtn.title = 'Pokaż filtry';
    }
}



// Update statistics (for internal use)
function updateStatsModal() {
    const prices = allOffers.map(offer => offer.najem_pln).filter(price => price > 0);
    const areas = allOffers.map(offer => parseFloat(offer.metraz_m2)).filter(area => area > 0);
    
    if (prices.length > 0) {
        const avgPrice = Math.round(prices.reduce((a, b) => a + b, 0) / prices.length);
        const minPrice = Math.round(Math.min(...prices));
        const maxPrice = Math.round(Math.max(...prices));
        
        document.getElementById('avg-price-stat').textContent = `${avgPrice} PLN`;
        document.getElementById('min-price-stat').textContent = `${minPrice} PLN`;
        document.getElementById('max-price-stat').textContent = `${maxPrice} PLN`;
        document.getElementById('total-offers-stat').textContent = allOffers.length;
        
        // Metraż statistics
        if (areas.length > 0) {
            const avgArea = Math.round(areas.reduce((a, b) => a + b, 0) / areas.length);
            const minArea = Math.round(Math.min(...areas));
            const maxArea = Math.round(Math.max(...areas));
            
            document.getElementById('avg-area-stat').textContent = `${avgArea} m²`;
            document.getElementById('min-area-stat').textContent = `${minArea} m²`;
            document.getElementById('max-area-stat').textContent = `${maxArea} m²`;
        }
        
        // District statistics
        updateDistrictChart();
    }
}

// Update district chart
function updateDistrictChart() {
    const districtCounts = {};
    allOffers.forEach(offer => {
        if (offer.dzielnica) {
            districtCounts[offer.dzielnica] = (districtCounts[offer.dzielnica] || 0) + 1;
        }
    });
    
    const chartContainer = document.getElementById('district-chart');
    const sortedDistricts = Object.entries(districtCounts)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 10);
    
    if (sortedDistricts.length === 0) {
        chartContainer.innerHTML = '<div class="no-data">No district data available</div>';
        return;
    }
    
    let chartHTML = '<div class="district-list">';
    sortedDistricts.forEach(([district, count]) => {
        const percentage = ((count / allOffers.length) * 100).toFixed(1);
        chartHTML += `
            <div class="district-item">
                <div class="district-name">${district}</div>
                <div class="district-bar">
                    <div class="district-bar-fill" style="width: ${percentage}%"></div>
                </div>
                <div class="district-count">${count} (${percentage}%)</div>
            </div>
        `;
    });
    chartHTML += '</div>';
    
    chartContainer.innerHTML = chartHTML;
}

// Close modal
function closeModal() {
    document.getElementById('offer-modal').style.display = 'none';
}

// Apply filters
function applyFilters() {
    const minPrice = parseFloat(document.getElementById('min-price').value) || 0;
    const maxPrice = parseFloat(document.getElementById('max-price').value) || Infinity;
    const selectedDistrict = document.getElementById('district-filter').value;
    
    filteredOffers = allOffers.filter(offer => {
        const price = offer.najem_pln || 0;
        const district = offer.dzielnica || '';
        
        const priceMatch = price >= minPrice && price <= maxPrice;
        const districtMatch = !selectedDistrict || district === selectedDistrict;
        
        return priceMatch && districtMatch;
    });
    
    updateMap();
    updateTotalOffers();
}

// Clear filters
function clearFilters() {
    document.getElementById('min-price').value = '';
    document.getElementById('max-price').value = '';
    document.getElementById('district-filter').value = '';
    
    filteredOffers = [...allOffers];
    updateMap();
    updateTotalOffers();
}



// Populate district filter
function populateDistrictFilter() {
    const districts = [...new Set(allOffers.map(offer => offer.dzielnica).filter(Boolean))].sort();
    const select = document.getElementById('district-filter');
    
    districts.forEach(district => {
        const option = document.createElement('option');
        option.value = district;
        option.textContent = district;
        select.appendChild(option);
    });
}

// Update total offers count
function updateTotalOffers() {
    document.getElementById('total-offers').textContent = filteredOffers.length;
}
