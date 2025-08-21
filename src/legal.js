// Initialize the legal page
document.addEventListener('DOMContentLoaded', function() {
    loadData();
});

// Load data from CSV file to get total offers count
function loadData() {
    Papa.parse('../data/oferty_geo.csv', {
        download: true,
        header: true,
        complete: function(results) {
            const allOffers = results.data.filter(offer => 
                offer.lat && offer.lon && 
                !isNaN(parseFloat(offer.lat)) && 
                !isNaN(parseFloat(offer.lon))
            );
            
            updateTotalOffers(allOffers.length);
        },
        error: function(error) {
            console.error('Error loading CSV:', error);
            document.getElementById('total-offers').textContent = '0';
        }
    });
}

// Update total offers count
function updateTotalOffers(count) {
    document.getElementById('total-offers').textContent = count;
}
