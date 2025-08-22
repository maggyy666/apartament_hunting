console.log("[BUILD]", "offers-without-coords.js loaded at", new Date().toISOString());

// Load data from CSV file
function loadData() {
    const csvUrl = '/data/oferty_geo.csv?nocache=' + Date.now();
    console.log("=== Loading CSV file ===", csvUrl);

    Papa.parse(csvUrl, {
        download: true,
        header: true,
        skipEmptyLines: true,
        complete: function(results) {
            console.log("=== Papa.parse results ===");
            console.log("rows (Papa):", results.data.length);
            
            // Filter offers without coordinates
            const offersWithoutCoords = results.data.filter(offer => {
                const lat = parseFloat(offer.lat);
                const lon = parseFloat(offer.lon);
                return !Number.isFinite(lat) || !Number.isFinite(lon);
            });
            
            console.log("Offers without coordinates:", offersWithoutCoords.length);
            
            // Update total count
            document.getElementById('total-offers-without-coords').textContent = offersWithoutCoords.length;
            
            // Update navbar count with total offers
            document.getElementById('total-offers').textContent = results.data.length;
            
            // Display offers
            displayOffersWithoutCoords(offersWithoutCoords);
        },
        error: function(error) {
            console.error('=== Error loading CSV ===', error);
            document.getElementById('offers-without-coords-list').innerHTML = 
                '<p class="error">Błąd podczas ładowania danych: ' + error.message + '</p>';
        }
    });
}

// Display offers without coordinates
function displayOffersWithoutCoords(offers) {
    const container = document.getElementById('offers-without-coords-list');
    
    if (offers.length === 0) {
        container.innerHTML = '<p class="no-offers">Wszystkie oferty mają przypisane współrzędne geograficzne!</p>';
        return;
    }
    
    const offersHtml = offers.map(offer => {
        const price = offer.najem_pln || 'Brak ceny';
        const adminFee = offer.czynsz_adm_pln ? ` + ${offer.czynsz_adm_pln} PLN (admin)` : '';
        const totalPrice = (parseFloat(offer.najem_pln || 0) + parseFloat(offer.czynsz_adm_pln || 0)) || 'Brak ceny';
        const district = offer.dzielnica || 'Brak dzielnicy';
        const address = offer.ulica || 'Brak adresu';
        const area = offer.metraz_m2 ? `${offer.metraz_m2}m²` : 'Brak metrażu';
        const url = offer.url || '#';
        
        return `
            <div class="offer-item">
                <div class="offer-header">
                    <h3 class="offer-title">
                        <a href="${url}" target="_blank" rel="noopener noreferrer">
                            ${offer.title || 'Brak tytułu'}
                        </a>
                    </h3>
                    <div class="offer-price">${totalPrice} PLN${adminFee}</div>
                </div>
                <div class="offer-details">
                    <p><strong>Adres:</strong> ${address}</p>
                    <p><strong>Dzielnica:</strong> ${district}</p>
                    <p><strong>Metraż:</strong> ${area}</p>
                    <p><strong>Współrzędne:</strong> <span class="no-coords">Brak współrzędnych</span></p>
                </div>
            </div>
        `;
    }).join('');
    
    container.innerHTML = offersHtml;
}

// Initialize the page
document.addEventListener('DOMContentLoaded', function() {
    loadData();
});
