console.log("[BUILD]", "legal.js loaded at", new Date().toISOString());

// Initialize the legal page
document.addEventListener('DOMContentLoaded', function() {
    loadData();
});

// Load data from CSV file to get total offers count
function loadData() {
    const csvUrl = '/data/oferty_geo.csv?nocache=' + Date.now();
    console.log("=== Loading CSV file ===", csvUrl);
    
    Papa.parse(csvUrl, {
        download: true,
        header: true,
        complete: function(results) {
            console.log("=== Papa.parse results ===");
            console.log("rows (Papa):", results.data.length);
            
            const allOffers = results.data.filter(offer => 
                offer.lat && offer.lon && 
                !isNaN(parseFloat(offer.lat)) && 
                !isNaN(parseFloat(offer.lon))
            );
            
            console.log("Offers with coordinates:", allOffers.length);
            updateTotalOffers(allOffers.length);
        },
        error: function(error) {
            console.error('=== Error loading CSV ===', error);
            document.getElementById('total-offers').textContent = '0';
        }
    });
}

// Update total offers count
function updateTotalOffers(count) {
    document.getElementById('total-offers').textContent = count;
}
