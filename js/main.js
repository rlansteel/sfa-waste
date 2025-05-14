/**
 * Archivo principal que coordina la aplicación
 */

// Variables de estado de la aplicación
let currentListView = 'countries';

// Inicialización cuando el DOM está completamente cargado
document.addEventListener('DOMContentLoaded', function() {
    debug("DOM completamente cargado. Iniciando aplicación...");

    // Elementos del DOM
    const searchInput = document.getElementById('search-input');
    const regionSelect = document.getElementById('region-select');
    const incomeSelect = document.getElementById('income-select');
    const sortSelect = document.getElementById('sort-select');
    const mapColorSelect = document.getElementById('map-color-select');
    const viewCountriesBtn = document.getElementById('view-countries-btn');
    const viewCitiesBtn = document.getElementById('view-cities-btn');
    const dateElement = document.getElementById('current-date');

    // Mostrar fecha actual
    if (dateElement) {
        dateElement.textContent = new Date().toLocaleDateString('es-ES', {
            year: 'numeric',
            month: 'long',
            day: 'numeric'
        });
    }

    // Inicializar mapa
    initMap();

    // Cargar datos iniciales con animación de carga
    animateLoading();
    loadInitialData()
        .then((initialData) => { // Recibe el objeto completo
            debug("Datos iniciales cargados con éxito");

            // Rellenar selectores
            populateSelect(regionSelect, initialData.regions);
            populateSelect(incomeSelect, initialData.incomeGroups);

            // Poblar tablas de resumen si los datos están disponibles
            if (initialData.summary_by_income_group) {
                populateSummaryTable('summary-by-income-body', initialData.summary_by_income_group, 'income_group', [
                    { key: 'avg_waste_per_capita', unit: ' kg/día', decimals: 2, label: 'Residuos P.C.' },
                    { key: 'avg_data_quality', unit: '%', decimals: 1, label: 'Calidad Datos' },
                    { key: 'avg_recycling_rate', unit: '%', decimals: 1, label: 'Tasa Reciclaje' },
                    { key: 'avg_collection_coverage', unit: '%', decimals: 1, label: 'Cobertura Colección' },
                    { key: 'avg_organic_composition', unit: '%', decimals: 1, label: 'Comp. Orgánica' }
                ], 'income_group_label');
            }
            if (initialData.summary_by_region) {
                populateSummaryTable('summary-by-region-body', initialData.summary_by_region, 'region', [
                    { key: 'avg_waste_per_capita', unit: ' kg/día', decimals: 2, label: 'Residuos P.C.' },
                    { key: 'avg_data_quality', unit: '%', decimals: 1, label: 'Calidad Datos' },
                    { key: 'avg_recycling_rate', unit: '%', decimals: 1, label: 'Tasa Reciclaje' },
                    { key: 'avg_collection_coverage', unit: '%', decimals: 1, label: 'Cobertura Colección' },
                    { key: 'avg_organic_composition', unit: '%', decimals: 1, label: 'Comp. Orgánica' }
                ]);
            }

            // Aplicar filtros iniciales y actualizar tabla Top 10
            handleFilterChange();

            // Configurar controladores de eventos
            setupEventListeners();
        })
        .catch(error => {
            handleDataLoadError(error);
        });
});

/**
 * Configura los controladores de eventos para la interfaz
 */
function setupEventListeners() {
    const searchInput = document.getElementById('search-input');
    const regionSelect = document.getElementById('region-select');
    const incomeSelect = document.getElementById('income-select');
    const sortSelect = document.getElementById('sort-select');
    const mapColorSelect = document.getElementById('map-color-select');
    const viewCountriesBtn = document.getElementById('view-countries-btn');
    const viewCitiesBtn = document.getElementById('view-cities-btn');

    // Controladores para filtros con debounce para búsqueda
    let searchTimeout;
    if (searchInput) {
        searchInput.addEventListener('input', function() {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                handleFilterChange();
            }, 300);
        });
    }

    if (regionSelect) regionSelect.addEventListener('change', handleFilterChange);
    if (incomeSelect) incomeSelect.addEventListener('change', handleFilterChange);
    if (sortSelect) sortSelect.addEventListener('change', handleFilterChange);
    if (mapColorSelect) mapColorSelect.addEventListener('change', handleFilterChange);

    // Controladores para cambio de vista con animación
    if (viewCountriesBtn) {
        viewCountriesBtn.addEventListener('click', () => {
            if (currentListView !== 'countries') {
                currentListView = 'countries';
                toggleViewButtons(viewCountriesBtn, viewCitiesBtn);
                animateListTransition();
                handleFilterChange();
            }
        });
    }

    if (viewCitiesBtn) {
        viewCitiesBtn.addEventListener('click', () => {
            if (currentListView !== 'cities') {
                currentListView = 'cities';
                toggleViewButtons(viewCitiesBtn, viewCountriesBtn);
                animateListTransition();
                handleFilterChange();
            }
        });
    }

    // Evento para redimensionar gráficos al cambiar tamaño de ventana
    window.addEventListener('resize', function() {
        clearTimeout(window.resizeTimer);
        window.resizeTimer = setTimeout(function() {
            const plotlyContainers = ['boxplot-container', 'scatter-gdp-container', 'quality-hist-container'];
            plotlyContainers.forEach(containerId => {
                const container = document.getElementById(containerId);
                if (container && container.style.display !== 'none' && typeof Plotly !== 'undefined') {
                    Plotly.relayout(containerId, {
                        'width': container.offsetWidth
                    }).catch(err => console.warn(`Error redimensionando gráfico ${containerId}:`, err));
                }
            });
        }, 250);
    });
}

/**
 * Anima la transición entre listas
 */
function animateListTransition() {
    const entityList = document.getElementById('entity-list');
    if (entityList) {
        entityList.style.opacity = '0';
        entityList.style.transform = 'translateY(10px)';

        setTimeout(() => {
            entityList.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
            entityList.style.opacity = '1';
            entityList.style.transform = 'translateY(0)';

            setTimeout(() => {
                entityList.style.transition = '';
            }, 300);
        }, 100);
    }
}

/**
 * Cambia las clases de los botones de toggle
 * @param {HTMLElement} activeBtn - Botón a activar
 * @param {HTMLElement} inactiveBtn - Botón a desactivar
 */
function toggleViewButtons(activeBtn, inactiveBtn) {
    if (activeBtn) {
        activeBtn.classList.remove('inactive');
        activeBtn.classList.add('active');
    }
    if (inactiveBtn) {
        inactiveBtn.classList.remove('active');
        inactiveBtn.classList.add('inactive');
    }
}

/**
 * Maneja cambios en los filtros y actualiza la visualización
 */
function handleFilterChange() {
    // Obtener valores de filtros
    const searchInput = document.getElementById('search-input');
    const regionSelect = document.getElementById('region-select');
    const incomeSelect = document.getElementById('income-select');
    const sortSelect = document.getElementById('sort-select');

    const searchTerm = searchInput ? searchInput.value.toLowerCase().trim() : '';
    const selectedRegion = regionSelect ? regionSelect.value : '';
    const selectedIncome = incomeSelect ? incomeSelect.value : '';
    const sortValue = sortSelect ? sortSelect.value : 'name_asc';

    // Animar cambios
    animateFilterChange();

    // Aplicar filtros
    const filters = { searchTerm, selectedRegion, selectedIncome, sortValue };
    // Correctly destructure 'groupedEntities' from the result of applyFilters
    const { groupedEntities, countriesMatchingFilters } = applyFilters(currentListView, filters);

    // Actualizar visualizaciones
    // Pass the correctly destructured 'groupedEntities' to updateDisplayedEntities
    updateDisplayedEntities(groupedEntities, currentListView);
    updateMapVisualization(countriesMatchingFilters);
    updateAllCharts(countriesMatchingFilters);
    updateTopWasteGeneratorsTable(countriesMatchingFilters);
}

/**
 * Anima cambios de filtro con efectos visuales
 */
function animateFilterChange() {
    const elements = ['#map', '#entity-list', '.chart-container-plotly', '#top-waste-generators-body', '#summary-by-income-body', '#summary-by-region-body'];

    elements.forEach(selector => {
        const els = document.querySelectorAll(selector);
        els.forEach(el => {
            el.style.transition = 'opacity 0.15s ease';
            el.style.opacity = '0.7';

            setTimeout(() => {
                el.style.opacity = '1';
                setTimeout(() => {
                    el.style.transition = '';
                }, 150);
            }, 150);
        });
    });
}

/**
 * Anima los elementos de carga
 */
function animateLoading() {
    const loadingElements = document.querySelectorAll('.loading-indicator');

    loadingElements.forEach(el => {
        el.style.opacity = '0';
        el.style.display = 'flex';

        setTimeout(() => {
            el.style.transition = 'opacity 0.3s ease';
            el.style.opacity = '1';
        }, 10);
    });
}

/**
 * Maneja errores en la carga de datos
 * @param {Error} error - Error ocurrido
 */
function handleDataLoadError(error) {
    const loadingElements = document.querySelectorAll('.loading-indicator');

    loadingElements.forEach(el => {
        el.style.opacity = '0';
        setTimeout(() => {
            el.style.display = 'none';
        }, 300);
    });

    const mapContainer = document.getElementById('map');
    // Use UI_DATA_DIR from config.js if available, otherwise use a placeholder.
    // Ensure UI_DATA_DIR is accessible here or pass it if needed.
    const dataDir = (typeof UI_DATA_DIR !== 'undefined') ? UI_DATA_DIR : './output/ui_data/';
    let errorMsg = `Error crítico al cargar datos: ${error.message}. Revisa la consola (F12) y verifica las rutas a '${dataDir}countries_polygons.geojson' y '${dataDir}index.json'.`;
    console.error('Error en Promise.all o procesamiento:', error); // Log the full error object

    if (mapContainer) {
        mapContainer.innerHTML = `
            <div class="flex flex-col items-center justify-center h-full p-6 text-center">
                <svg class="h-12 w-12 text-red-500 mb-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
                <p class="text-red-600 font-medium text-lg mb-2">Error al cargar datos del mapa</p>
                <p class="text-slate-600 text-sm">${errorMsg}</p>
            </div>`;
    }

    const chartContainers = ['boxplot-container', 'scatter-gdp-container', 'quality-hist-container'];
    chartContainers.forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.innerHTML = `
                <div class="flex flex-col items-center justify-center h-full p-6 text-center">
                    <svg class="h-8 w-8 text-red-500 mb-3" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                    <p class="text-slate-600">Error al cargar datos para el gráfico.</p>
                </div>`;
        }
    });

    const noResultsElement = document.getElementById('no-results');
    if (noResultsElement) {
        noResultsElement.innerHTML = `
            <div class="flex items-center">
                <svg class="h-5 w-5 text-red-500 mr-2" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
                    <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clip-rule="evenodd" />
                </svg>
                <span>No se pudieron cargar los datos iniciales. Intenta recargar la página y revisa la consola.</span>
            </div>`;
        noResultsElement.classList.remove('hidden');
        noResultsElement.classList.add('bg-red-50', 'border-red-200', 'text-red-800');
    }
}
