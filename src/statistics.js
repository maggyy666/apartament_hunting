// Global variables
let allOffers = [];
let chartInstances = {}; // Store chart references to prevent duplication

// Initialize the statistics page
document.addEventListener('DOMContentLoaded', function() {
    loadData();
    setupNavigation();
});

// Load data from CSV file
function loadData() {
    const csvUrl = '/data/oferty_geo.csv?nocache=' + Date.now();
    console.log("=== Loading CSV file ===", csvUrl);
    
    Papa.parse(csvUrl, {
        download: true,
        header: true,
        complete: function(results) {
            // Deduplicate offers by ID
            const offerMap = new Map();
            results.data.forEach(offer => {
                if (offer.id && !offerMap.has(offer.id)) {
                    offerMap.set(offer.id, offer);
                }
            });
            
            allOffers = [...offerMap.values()].filter(offer => 
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
            
            updateTotalOffers();
            updateStatsModal();
        },
        error: function(error) {
            console.error('Error loading CSV:', error);
            alert('Error loading data. Check if CSV file exists.');
        }
    });
}

// Update total offers count
function updateTotalOffers() {
    document.getElementById('total-offers').textContent = allOffers.length;
}

// Update statistics in page
function updateStatsModal() {
    const rents = allOffers.map(offer => offer.najem_pln).filter(price => price > 0);
    const areas = allOffers.map(offer => parseFloat(offer.metraz_m2)).filter(area => area > 0);
    const adminFees = allOffers.map(offer => offer.czynsz_adm_pln).filter(fee => fee > 0);
    
    if (rents.length > 0) {
        // Basic rent statistics
        const avgRent = Math.round(rents.reduce((a, b) => a + b, 0) / rents.length);
        const medianRent = calculateMedian(rents);
        
        document.getElementById('avg-rent').textContent = avgRent.toLocaleString();
        document.getElementById('median-rent').textContent = medianRent.toLocaleString();
        document.getElementById('total-offers').textContent = allOffers.length;
        
        // Area statistics
        if (areas.length > 0) {
            const avgArea = Math.round(areas.reduce((a, b) => a + b, 0) / areas.length);
            document.getElementById('avg-area').textContent = avgArea;
        }
        
        // Price per m² calculations
        const offersWithArea = allOffers.filter(offer => 
            offer.najem_pln > 0 && offer.metraz_m2 > 0
        );
        
        if (offersWithArea.length > 0) {
            const pricePerM2 = offersWithArea.map(offer => offer.najem_pln / offer.metraz_m2);
            const totalPerM2 = offersWithArea.map(offer => 
                (offer.najem_pln + (offer.czynsz_adm_pln || 0)) / offer.metraz_m2
            );
            
            const avgPricePerM2 = Math.round(pricePerM2.reduce((a, b) => a + b, 0) / pricePerM2.length);
            const avgTotalPerM2 = Math.round(totalPerM2.reduce((a, b) => a + b, 0) / totalPerM2.length);
            
            document.getElementById('avg-price-per-m2').textContent = avgPricePerM2.toLocaleString();
            document.getElementById('avg-total-per-m2').textContent = avgTotalPerM2.toLocaleString();
        }
        
                 // Admin fee statistics - fixed calculation
         const offersWithBoth = allOffers.filter(offer => 
             offer.najem_pln > 0 && offer.czynsz_adm_pln > 0
         );
         
         if (offersWithBoth.length > 0) {
             const adminSum = offersWithBoth.reduce((sum, offer) => sum + offer.czynsz_adm_pln, 0);
             const totalSum = offersWithBoth.reduce((sum, offer) => sum + offer.najem_pln + offer.czynsz_adm_pln, 0);
             const adminFeeShare = Math.round((adminSum / totalSum) * 100);
             document.getElementById('admin-fee-share').textContent = adminFeeShare + '%';
             
             // Update admin fee stats
             const adminFees = offersWithBoth.map(o => o.czynsz_adm_pln);
             const avgAdminFee = Math.round(adminFees.reduce((a, b) => a + b, 0) / adminFees.length);
             const medianAdminFee = Math.round(calculateMedian(adminFees));
             const minAdminFee = Math.min(...adminFees);
             const maxAdminFee = Math.max(...adminFees);
             
             document.getElementById('avg-admin-fee').textContent = avgAdminFee.toLocaleString();
             document.getElementById('median-admin-fee').textContent = medianAdminFee.toLocaleString();
             document.getElementById('admin-fee-range').textContent = `${minAdminFee.toLocaleString()} - ${maxAdminFee.toLocaleString()}`;
             
             // Admin fee per m²
             const offersWithAreaAndFee = offersWithBoth.filter(offer => offer.metraz_m2 > 0);
             if (offersWithAreaAndFee.length > 0) {
                 const adminFeePerM2 = offersWithAreaAndFee.map(offer => offer.czynsz_adm_pln / offer.metraz_m2);
                 const avgAdminFeePerM2 = Math.round(adminFeePerM2.reduce((a, b) => a + b, 0) / adminFeePerM2.length);
                 document.getElementById('admin-fee-per-m2').textContent = avgAdminFeePerM2.toLocaleString();
             }
         }
        
        // Data quality
        const completeRecords = allOffers.filter(offer => 
            offer.najem_pln > 0 && offer.metraz_m2 > 0 && offer.lat && offer.lon
        ).length;
        const dataQuality = Math.round((completeRecords / allOffers.length) * 100);
        document.getElementById('data-quality').textContent = dataQuality + '%';
        
        // Update tables and charts
        updateDistrictTable();
        updateCharts();
    }
}

// Helper function to calculate median (without rounding)
function calculateMedian(arr) {
    const sorted = arr.slice().sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 0) {
        return (sorted[middle - 1] + sorted[middle]) / 2;
    }
    return sorted[middle];
}

// Helper function to calculate percentiles
function calculatePercentile(arr, percentile) {
    const sorted = arr.slice().sort((a, b) => a - b);
    const index = (percentile / 100) * (sorted.length - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    const weight = index - lower;
    
    if (upper === lower) return sorted[lower];
    return sorted[lower] * (1 - weight) + sorted[upper] * weight;
}

// Helper function to create chart safely (destroy previous instance)
function createChart(canvasId, config) {
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
    }
    const ctx = document.getElementById(canvasId)?.getContext('2d');
    if (!ctx) return null;
    
    chartInstances[canvasId] = new Chart(ctx, config);
    return chartInstances[canvasId];
}

// Update district table
function updateDistrictTable() {
    const districtStats = {};
    
    allOffers.forEach(offer => {
        if (!offer.dzielnica) return;
        
        if (!districtStats[offer.dzielnica]) {
            districtStats[offer.dzielnica] = {
                offers: [],
                rents: [],
                areas: [],
                pricePerM2: [],
                totalPerM2: [],
                adminFees: []
            };
        }
        
        districtStats[offer.dzielnica].offers.push(offer);
        
        if (offer.najem_pln > 0) {
            districtStats[offer.dzielnica].rents.push(offer.najem_pln);
        }
        
        if (offer.metraz_m2 > 0) {
            districtStats[offer.dzielnica].areas.push(offer.metraz_m2);
        }
        
        if (offer.najem_pln > 0 && offer.metraz_m2 > 0) {
            districtStats[offer.dzielnica].pricePerM2.push(offer.najem_pln / offer.metraz_m2);
            const total = offer.najem_pln + (offer.czynsz_adm_pln || 0);
            districtStats[offer.dzielnica].totalPerM2.push(total / offer.metraz_m2);
        }
        
        if (offer.czynsz_adm_pln > 0) {
            districtStats[offer.dzielnica].adminFees.push(offer.czynsz_adm_pln);
        }
    });
    
    const tableBody = document.getElementById('district-table-body');
    let tableHTML = '';
    
    Object.entries(districtStats)
        .sort(([,a], [,b]) => b.offers.length - a.offers.length)
        .forEach(([district, stats]) => {
            const medianRent = stats.rents.length > 0 ? calculateMedian(stats.rents) : 0;
            const medianArea = stats.areas.length > 0 ? Math.round(calculateMedian(stats.areas)) : 0;
            const medianPricePerM2 = stats.pricePerM2.length > 0 ? Math.round(calculateMedian(stats.pricePerM2)) : 0;
            const medianTotalPerM2 = stats.totalPerM2.length > 0 ? Math.round(calculateMedian(stats.totalPerM2)) : 0;
            
                         // Calculate admin fee share - fixed calculation
             let adminFeeShare = 0;
             const offersWithBoth = stats.offers.filter(o => o.najem_pln > 0 && o.czynsz_adm_pln > 0);
             if (offersWithBoth.length > 0) {
                 const adminSum = offersWithBoth.reduce((sum, o) => sum + o.czynsz_adm_pln, 0);
                 const totalSum = offersWithBoth.reduce((sum, o) => sum + o.najem_pln + o.czynsz_adm_pln, 0);
                 adminFeeShare = Math.round((adminSum / totalSum) * 100);
             }
            
            tableHTML += `
                <tr>
                    <td>${district}</td>
                    <td>${stats.offers.length}</td>
                    <td>${medianRent.toLocaleString()}</td>
                    <td>${medianArea}</td>
                    <td>${medianPricePerM2.toLocaleString()}</td>
                    <td>${medianTotalPerM2.toLocaleString()}</td>
                    <td>${adminFeeShare}%</td>
                </tr>
            `;
        });
    
    tableBody.innerHTML = tableHTML;
}

// Setup navigation
function setupNavigation() {
    const tabs = document.querySelectorAll('.nav-tab');
    const sections = document.querySelectorAll('.stats-section');
    
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const targetSection = tab.getAttribute('data-section');
            
            // Update active tab
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            
            // Show target section
            sections.forEach(section => {
                section.classList.remove('active');
                if (section.id === targetSection) {
                    section.classList.add('active');
                }
            });
            
            // Update charts for the active section
            updateSectionCharts(targetSection);
        });
    });
}

// Update charts
function updateCharts() {
    updatePriceChart();
    updateScatterChart();
    updateDistrictChart();
    updateAdminFeeChart();
    updateAreaDistributionChart();
    updateGeoScatterChart();
    updateDistanceChart();
}

// Update charts for specific section
function updateSectionCharts(section) {
    switch(section) {
        case 'districts':
            updateDistrictChart();
            break;
        case 'prices':
            updatePriceChart();
            updatePriceBoxplot();
            break;
        case 'areas':
            updateScatterChart();
            updateAreaDistributionChart();
            break;
        case 'admin-fees':
            updateAdminFeeChart();
            break;
        case 'geography':
            updateGeoScatterChart();
            updateDistanceChart();
            updateRingsChart();
            break;
        case 'insights':
            updatePriceSegments();
            updateDealsTable();
            updateDataQualityChart();
            break;
    }
}

// Price distribution chart
function updatePriceChart() {
    const rents = allOffers.map(offer => offer.najem_pln).filter(price => price > 0);
    
    if (rents.length === 0) return;
    
    // Create histogram data with protection against equal values
    const maxPrice = Math.max(...rents);
    const minPrice = Math.min(...rents);
    const binCount = 20;
    const binSize = (maxPrice - minPrice) / binCount || 1;
    
    const bins = new Array(binCount).fill(0);
    rents.forEach(price => {
        const binIndex = Math.min(Math.floor((price - minPrice) / binSize), binCount - 1);
        bins[binIndex]++;
    });
    
    const labels = bins.map((_, i) => {
        const start = minPrice + i * binSize;
        const end = minPrice + (i + 1) * binSize;
        return `${Math.round(start).toLocaleString()} - ${Math.round(end).toLocaleString()}`;
    });
    
    createChart('price-chart', {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Number of Offers',
                data: bins,
                backgroundColor: 'rgba(102, 126, 234, 0.6)',
                borderColor: 'rgba(102, 126, 234, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Rent Price Distribution'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Number of Offers'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Rent Price (PLN/month)'
                    }
                }
            }
        }
    });
}

// Area vs Price scatter chart
function updateScatterChart() {
    const offersWithArea = allOffers.filter(offer => 
        offer.najem_pln > 0 && offer.metraz_m2 > 0
    );
    
    if (offersWithArea.length === 0) return;
    
    const data = offersWithArea.map(offer => ({
        x: offer.metraz_m2,
        y: offer.najem_pln
    }));
    
    createChart('scatter-chart', {
        type: 'scatter',
        data: {
            datasets: [{
                label: 'Offers',
                data: data,
                backgroundColor: 'rgba(102, 126, 234, 0.6)',
                borderColor: 'rgba(102, 126, 234, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Area vs Rent Price'
                }
            },
            scales: {
                y: {
                    title: {
                        display: true,
                        text: 'Rent Price (PLN/month)'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Area (m²)'
                    }
                }
            }
        }
    });
}

// District comparison chart
function updateDistrictChart() {
    const districtStats = {};
    
    allOffers.forEach(offer => {
        if (!offer.dzielnica) return;
        
        if (!districtStats[offer.dzielnica]) {
            districtStats[offer.dzielnica] = {
                offers: [],
                totalPerM2: []
            };
        }
        
        districtStats[offer.dzielnica].offers.push(offer);
        
        if (offer.najem_pln > 0 && offer.metraz_m2 > 0) {
            const total = offer.najem_pln + (offer.czynsz_adm_pln || 0);
            districtStats[offer.dzielnica].totalPerM2.push(total / offer.metraz_m2);
        }
    });
    
    const districts = Object.entries(districtStats)
        .filter(([, stats]) => stats.totalPerM2.length > 0)
        .sort(([,a], [,b]) => calculateMedian(b.totalPerM2) - calculateMedian(a.totalPerM2))
        .slice(0, 10);
    
    if (districts.length === 0) return;
    
    const labels = districts.map(([district]) => district);
    const data = districts.map(([, stats]) => calculateMedian(stats.totalPerM2));
    
    createChart('district-chart', {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Median Total Price/m²',
                data: data,
                backgroundColor: 'rgba(118, 75, 162, 0.6)',
                borderColor: 'rgba(118, 75, 162, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Price per m² by District'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Price per m² (PLN)'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'District'
                    }
                }
            }
        }
    });
}

// Admin fee distribution chart
function updateAdminFeeChart() {
    const adminFees = allOffers.map(offer => offer.czynsz_adm_pln).filter(fee => fee > 0);
    
    if (adminFees.length === 0) return;
    
    // Create histogram data with protection against equal values
    const maxFee = Math.max(...adminFees);
    const minFee = Math.min(...adminFees);
    const binCount = 15;
    const binSize = (maxFee - minFee) / binCount || 1;
    
    const bins = new Array(binCount).fill(0);
    adminFees.forEach(fee => {
        const binIndex = Math.min(Math.floor((fee - minFee) / binSize), binCount - 1);
        bins[binIndex]++;
    });
    
    const labels = bins.map((_, i) => {
        const start = minFee + i * binSize;
        const end = minFee + (i + 1) * binSize;
        return `${Math.round(start).toLocaleString()} - ${Math.round(end).toLocaleString()}`;
    });
    
    createChart('admin-fee-chart', {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Number of Offers',
                data: bins,
                backgroundColor: 'rgba(255, 99, 132, 0.6)',
                borderColor: 'rgba(255, 99, 132, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Administrative Fee Distribution'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Number of Offers'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Administrative Fee (PLN/month)'
                    }
                }
            }
        }
    });
}

// Area distribution chart
function updateAreaDistributionChart() {
    const areas = allOffers.map(offer => offer.metraz_m2).filter(area => area > 0);
    
    if (areas.length === 0) return;
    
    // Create histogram data with protection against equal values
    const maxArea = Math.max(...areas);
    const minArea = Math.min(...areas);
    const binCount = 15;
    const binSize = (maxArea - minArea) / binCount || 1;
    
    const bins = new Array(binCount).fill(0);
    areas.forEach(area => {
        const binIndex = Math.min(Math.floor((area - minArea) / binSize), binCount - 1);
        bins[binIndex]++;
    });
    
    const labels = bins.map((_, i) => {
        const start = minArea + i * binSize;
        const end = minArea + (i + 1) * binSize;
        return `${Math.round(start)} - ${Math.round(end)} m²`;
    });
    
    createChart('area-distribution', {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Number of Offers',
                data: bins,
                backgroundColor: 'rgba(75, 192, 192, 0.6)',
                borderColor: 'rgba(75, 192, 192, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Apartment Area Distribution'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Number of Offers'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Area (m²)'
                    }
                }
            }
        }
    });
}

// Price boxplot chart
function updatePriceBoxplot() {
    const rents = allOffers.map(offer => offer.najem_pln).filter(price => price > 0);
    
    if (rents.length === 0) return;
    
    // Calculate quartiles
    const sorted = rents.slice().sort((a, b) => a - b);
    const q1 = sorted[Math.floor(sorted.length * 0.25)];
    const q2 = calculateMedian(rents);
    const q3 = sorted[Math.floor(sorted.length * 0.75)];
    const min = Math.min(...rents);
    const max = Math.max(...rents);
    
    createChart('price-boxplot', {
        type: 'bar',
        data: {
            labels: ['Rent Price Distribution'],
            datasets: [{
                label: 'Min',
                data: [min],
                backgroundColor: 'rgba(255, 99, 132, 0.6)',
                borderColor: 'rgba(255, 99, 132, 1)',
                borderWidth: 1
            }, {
                label: 'Q1',
                data: [q1],
                backgroundColor: 'rgba(255, 159, 64, 0.6)',
                borderColor: 'rgba(255, 159, 64, 1)',
                borderWidth: 1
            }, {
                label: 'Median',
                data: [q2],
                backgroundColor: 'rgba(255, 205, 86, 0.6)',
                borderColor: 'rgba(255, 205, 86, 1)',
                borderWidth: 1
            }, {
                label: 'Q3',
                data: [q3],
                backgroundColor: 'rgba(75, 192, 192, 0.6)',
                borderColor: 'rgba(75, 192, 192, 1)',
                borderWidth: 1
            }, {
                label: 'Max',
                data: [max],
                backgroundColor: 'rgba(54, 162, 235, 0.6)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Rent Price Quartiles'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Price (PLN/month)'
                    }
                }
            }
        }
    });
}

// Geographic scatter chart
function updateGeoScatterChart() {
    const offersWithGeo = allOffers.filter(offer => 
        offer.lat && offer.lon && offer.najem_pln > 0
    );
    
    if (offersWithGeo.length === 0) return;
    
    const data = offersWithGeo.map(offer => ({
        x: offer.lon,
        y: offer.lat,
        r: Math.min(offer.najem_pln / 100, 20) // Size based on price
    }));
    
    createChart('geo-scatter', {
        type: 'bubble',
        data: {
            datasets: [{
                label: 'Offers by Location',
                data: data,
                backgroundColor: 'rgba(102, 126, 234, 0.6)',
                borderColor: 'rgba(102, 126, 234, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Geographic Distribution of Offers'
                }
            },
            scales: {
                y: {
                    title: {
                        display: true,
                        text: 'Latitude'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Longitude'
                    }
                }
            }
        }
    });
}

// Distance from center chart
function updateDistanceChart() {
    // Kraków center coordinates
    const krakowCenter = { lat: 50.0647, lon: 19.9450 };
    
    const offersWithGeo = allOffers.filter(offer => 
        offer.lat && offer.lon && offer.najem_pln > 0
    );
    
    if (offersWithGeo.length === 0) return;
    
    // Calculate distances
    const distances = offersWithGeo.map(offer => {
        const distance = calculateDistance(
            krakowCenter.lat, krakowCenter.lon,
            offer.lat, offer.lon
        );
        return { distance, price: offer.najem_pln };
    });
    
    createChart('distance-chart', {
        type: 'scatter',
        data: {
            datasets: [{
                label: 'Price vs Distance from Center',
                data: distances.map(d => ({ x: d.distance, y: d.price })),
                backgroundColor: 'rgba(153, 102, 255, 0.6)',
                borderColor: 'rgba(153, 102, 255, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Price vs Distance from Kraków Center'
                }
            },
            scales: {
                y: {
                    title: {
                        display: true,
                        text: 'Rent Price (PLN/month)'
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Distance from Center (km)'
                    }
                }
            }
        }
    });
}

// Helper function to calculate distance between two points
function calculateDistance(lat1, lon1, lat2, lon2) {
    const R = 6371; // Earth's radius in km
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon/2) * Math.sin(dLon/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c;
}

// Price segments analysis
function updatePriceSegments() {
    const rents = allOffers.map(offer => offer.najem_pln).filter(price => price > 0);
    
    if (rents.length === 0) return;
    
    const budget = rents.filter(price => price <= 2000).length;
    const midRange = rents.filter(price => price > 2000 && price <= 3000).length;
    const premium = rents.filter(price => price > 3000 && price <= 4000).length;
    const luxury = rents.filter(price => price > 4000).length;
    
    document.getElementById('budget-count').textContent = budget;
    document.getElementById('mid-count').textContent = midRange;
    document.getElementById('premium-count').textContent = premium;
    document.getElementById('luxury-count').textContent = luxury;
    
    // Create price segments chart
    createChart('price-segments-chart', {
        type: 'doughnut',
        data: {
            labels: ['Budget (≤2000 PLN)', 'Mid-range (2001-3000 PLN)', 'Premium (3001-4000 PLN)', 'Luxury (>4000 PLN)'],
            datasets: [{
                data: [budget, midRange, premium, luxury],
                backgroundColor: [
                    'rgba(75, 192, 192, 0.6)',
                    'rgba(54, 162, 235, 0.6)',
                    'rgba(255, 205, 86, 0.6)',
                    'rgba(255, 99, 132, 0.6)'
                ],
                borderColor: [
                    'rgba(75, 192, 192, 1)',
                    'rgba(54, 162, 235, 1)',
                    'rgba(255, 205, 86, 1)',
                    'rgba(255, 99, 132, 1)'
                ],
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Price Distribution by Segments'
                },
                legend: {
                    position: 'bottom'
                }
            }
        }
    });
}

// Best deals table
function updateDealsTable() {
    const offersWithCompleteData = allOffers.filter(offer => 
        offer.najem_pln > 0 && offer.metraz_m2 > 0 && offer.dzielnica
    );
    
    if (offersWithCompleteData.length === 0) return;
    
    // Calculate value score (lower price/m² = better value)
    const offersWithScore = offersWithCompleteData.map(offer => {
        const totalCost = offer.najem_pln + (offer.czynsz_adm_pln || 0);
        const pricePerM2 = totalCost / offer.metraz_m2;
        return {
            ...offer,
            totalCost,
            pricePerM2,
            valueScore: 1000 / pricePerM2 // Higher score = better value
        };
    });
    
    // Get top 10 deals
    const topDeals = offersWithScore
        .sort((a, b) => b.valueScore - a.valueScore)
        .slice(0, 10);
    
    const tableBody = document.getElementById('deals-table-body');
    let tableHTML = '';
    
    topDeals.forEach(deal => {
        tableHTML += `
            <tr>
                <td>${deal.dzielnica}</td>
                <td>${Math.round(deal.pricePerM2).toLocaleString()}</td>
                <td>${Math.round(deal.totalCost).toLocaleString()}</td>
                <td>${deal.metraz_m2}</td>
                <td>${Math.round(deal.valueScore)}</td>
            </tr>
        `;
    });
    
    tableBody.innerHTML = tableHTML;
}

// Data quality chart
function updateDataQualityChart() {
    const totalOffers = allOffers.length;
    const withRent = allOffers.filter(o => o.najem_pln > 0).length;
    const withArea = allOffers.filter(o => o.metraz_m2 > 0).length;
    const withAdminFee = allOffers.filter(o => o.czynsz_adm_pln > 0).length;
    const withDistrict = allOffers.filter(o => o.dzielnica).length;
    const withGeo = allOffers.filter(o => o.lat && o.lon).length;
    
    createChart('data-quality-chart', {
        type: 'bar',
        data: {
            labels: ['Rent Price', 'Area', 'Admin Fee', 'District', 'Coordinates'],
            datasets: [{
                label: 'Data Completeness (%)',
                data: [
                    Math.round((withRent / totalOffers) * 100),
                    Math.round((withArea / totalOffers) * 100),
                    Math.round((withAdminFee / totalOffers) * 100),
                    Math.round((withDistrict / totalOffers) * 100),
                    Math.round((withGeo / totalOffers) * 100)
                ],
                backgroundColor: 'rgba(102, 126, 234, 0.6)',
                borderColor: 'rgba(102, 126, 234, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Data Completeness by Field'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    title: {
                        display: true,
                        text: 'Percentage (%)'
                    }
                }
            }
        }
    });
}

// Rings chart (distance from center)
function updateRingsChart() {
    const krakowCenter = { lat: 50.0647, lon: 19.9450 };
    
    const offersWithGeo = allOffers.filter(offer => 
        offer.lat && offer.lon && offer.najem_pln > 0
    );
    
    if (offersWithGeo.length === 0) return;
    
    // Calculate distances and group into rings
    const distances = offersWithGeo.map(offer => {
        const distance = calculateDistance(
            krakowCenter.lat, krakowCenter.lon,
            offer.lat, offer.lon
        );
        return { distance, price: offer.najem_pln };
    });
    
    const ring0_1 = distances.filter(d => d.distance <= 1).length;
    const ring1_3 = distances.filter(d => d.distance > 1 && d.distance <= 3).length;
    const ring3_5 = distances.filter(d => d.distance > 3 && d.distance <= 5).length;
    const ring5_plus = distances.filter(d => d.distance > 5).length;
    
    createChart('rings-chart', {
        type: 'doughnut',
        data: {
            labels: ['0-1 km', '1-3 km', '3-5 km', '>5 km'],
            datasets: [{
                data: [ring0_1, ring1_3, ring3_5, ring5_plus],
                backgroundColor: [
                    'rgba(255, 99, 132, 0.6)',
                    'rgba(255, 159, 64, 0.6)',
                    'rgba(255, 205, 86, 0.6)',
                    'rgba(75, 192, 192, 0.6)'
                ],
                borderColor: [
                    'rgba(255, 99, 132, 1)',
                    'rgba(255, 159, 64, 1)',
                    'rgba(255, 205, 86, 1)',
                    'rgba(75, 192, 192, 1)'
                ],
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: {
                    display: true,
                    text: 'Offers by Distance from Center'
                },
                legend: {
                    position: 'bottom'
                }
            }
        }
    });
}
