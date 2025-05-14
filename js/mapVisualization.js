/**
 * Funcionalidad para la visualización del mapa
 */

// Variables globales para el mapa
let map = null;
let geoJsonCountriesLayer = null;
let cityMarkersLayer = null;
let selectedCountryLayer = null;
let mapLegend = null;
let tooltip = null;

/**
 * Inicializa el mapa Leaflet
 */
function initMap() {
    const mapElement = document.getElementById('map');
    const loadingMapElement = document.getElementById('loading-map');
    
    if (mapElement && !map) {
        try {
            debug("Inicializando mapa Leaflet...");
            map = L.map('map', {
                minZoom: 2,
                maxZoom: 18,
                zoomControl: true,
                attributionControl: true
            }).setView([20, 0], 2); 
            
            // Usando un mapa base más moderno
            L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
                subdomains: 'abcd',
                maxZoom: 19
            }).addTo(map);
            
            cityMarkersLayer = L.layerGroup().addTo(map);
            
            // Crear tooltip personalizado
            createTooltip();
            
            // Añadir control para la leyenda
            initMapLegend();
            
            // Añadir escala
            L.control.scale({
                imperial: false,
                position: 'bottomleft'
            }).addTo(map);
            
            debug("Mapa y capa de marcadores inicializados correctamente");
        } catch (e) { 
            console.error("Error inicializando Leaflet:", e);
            if(mapElement) mapElement.innerHTML = `
                <div class="flex flex-col items-center justify-center h-full p-6 text-center">
                    <svg class="h-12 w-12 text-red-500 mb-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                    </svg>
                    <p class="text-red-600 font-medium">Error al cargar el mapa.</p>
                </div>`;
            if(loadingMapElement) loadingMapElement.style.display = 'none';
        }
    }
}

/**
 * Crea un tooltip personalizado
 */
function createTooltip() {
    const tooltip = document.createElement('div');
    tooltip.className = 'custom-tooltip';
    tooltip.id = 'map-tooltip';
    document.body.appendChild(tooltip);
}

/**
 * Muestra el tooltip
 * @param {Object} e - Evento del mapa
 * @param {string} content - Contenido HTML para el tooltip
 */
function showTooltip(e, content) {
    const tooltip = document.getElementById('map-tooltip');
    if (!tooltip) return;
    
    tooltip.innerHTML = content;
    tooltip.style.opacity = '0';
    tooltip.style.transform = 'translateY(10px)';
    tooltip.style.display = 'block';
    
    const x = e.originalEvent.pageX + 15;
    const y = e.originalEvent.pageY - 30;
    
    tooltip.style.left = `${x}px`;
    tooltip.style.top = `${y}px`;
    
    setTimeout(() => {
        tooltip.classList.add('visible');
    }, 10);
}

/**
 * Oculta el tooltip
 */
function hideTooltip() {
    const tooltip = document.getElementById('map-tooltip');
    if (!tooltip) return;
    
    tooltip.classList.remove('visible');
    
    setTimeout(() => {
        tooltip.style.display = 'none';
    }, 300);
}

/**
 * Inicializa la leyenda del mapa
 */
function initMapLegend() {
    if (!map) return;
    
    if (mapLegend) {
        map.removeControl(mapLegend);
    }
    
    mapLegend = L.control({position: 'bottomright'});
    
    mapLegend.onAdd = function (map) {
        const div = L.DomUtil.create('div', 'map-legend');
        div.innerHTML = '<strong>Leyenda</strong><br>';
        div.id = 'map-legend-container';
        return div;
    };
    
    mapLegend.addTo(map);
}

/**
 * Actualiza la leyenda del mapa según el tipo de coloración
 * @param {string} colorBy - Tipo de coloración ('quality', 'region' o 'income')
 */
function updateMapLegend(colorBy) {
    const legendContainer = document.getElementById('map-legend-container');
    if (!legendContainer) return;
    
    let legendHTML = '<strong class="text-slate-800 font-semibold text-sm block mb-2">Leyenda</strong>';
    
    switch (colorBy) {
        case 'quality':
            legendHTML += createLegendItem(CONFIG.QUALITY_COLORS.high, 'Calidad Alta (≥75)');
            legendHTML += createLegendItem(CONFIG.QUALITY_COLORS.medium, 'Calidad Media (50-75)');
            legendHTML += createLegendItem(CONFIG.QUALITY_COLORS.low, 'Calidad Baja (<50)');
            legendHTML += createLegendItem(CONFIG.QUALITY_COLORS.default, 'Sin datos de calidad');
            break;
            
        case 'region':
            Object.entries(CONFIG.REGION_COLORS)
                .filter(([key]) => !['EAP', 'ECA', 'LAC', 'MENA', 'NA', 'SA', 'SSA', 'Default'].includes(key))
                .forEach(([region, color]) => {
                    legendHTML += createLegendItem(color, region);
                });
            break;
            
        case 'income':
            Object.entries(CONFIG.INCOME_GROUP_COLORS)
                .filter(([key]) => !['HIC', 'UMC', 'LMC', 'LIC', 'Default'].includes(key))
                .forEach(([income, color]) => {
                    legendHTML += createLegendItem(color, income);
                });
            break;
    }
    
    legendContainer.innerHTML = legendHTML;
}

/**
 * Crea un elemento de leyenda con color
 * @param {string} color - Código de color hex
 * @param {string} label - Etiqueta para el elemento
 * @returns {string} HTML para el elemento de leyenda
 */
function createLegendItem(color, label) {
    return `<div class="legend-item">
        <span class="legend-color" style="background-color: ${color}"></span>
        <span class="text-xs text-slate-700">${label}</span>
    </div>`;
}

/**
 * Aplica estilo a los países en el mapa según el tipo de coloración
 * @param {Object} feature - Feature GeoJSON del país
 * @returns {Object} Objeto de estilos para Leaflet
 */
function styleCountry(feature) {
    const isoA3 = feature.properties[GEOJSON_ISO_PROPERTY];
    const countryData = wasteDataCountries.find(c => c.iso3 === isoA3);
    let fillColor = CONFIG.REGION_COLORS['Default'];
    let fillOpacity = 0.4;
    
    const mapColorSelect = document.getElementById('map-color-select');
    const colorBy = mapColorSelect ? mapColorSelect.value : 'quality';

    if (countryData) {
        fillOpacity = 0.65;
        
        switch (colorBy) {
            case 'quality':
                if (countryData.data_quality_score !== null && countryData.data_quality_score !== undefined) {
                    const score = parseFloat(countryData.data_quality_score);
                    if (score >= 75) fillColor = CONFIG.QUALITY_COLORS.high;
                    else if (score >= 50) fillColor = CONFIG.QUALITY_COLORS.medium;
                    else fillColor = CONFIG.QUALITY_COLORS.low;
                } else {
                    fillColor = CONFIG.QUALITY_COLORS.default;
                }
                break;
                
            case 'region':
                if (countryData.region) {
                    const regionMappings = {
                        'East Asia & Pacific': 'EAP',
                        'Europe & Central Asia': 'ECA',
                        'Latin America & Caribbean': 'LAC',
                        'Middle East & North Africa': 'MENA',
                        'North America': 'NA',
                        'South Asia': 'SA',
                        'Sub-Saharan Africa': 'SSA'
                    };

                    // Normalizar la región
                    let regionKey = countryData.region;
                    
                    // Si es un nombre largo, obtener su código
                    if (regionMappings[regionKey]) {
                        regionKey = regionMappings[regionKey];
                    }
                    // Si es un código, buscar directamente
                    else if (Object.values(regionMappings).includes(regionKey)) {
                        // Ya tenemos el código correcto
                    }
                    // Si no coincide con ninguno, buscar coincidencia parcial
                    else {
                        const matchingRegion = Object.entries(regionMappings)
                            .find(([name, code]) => 
                                name.includes(regionKey) || 
                                regionKey.includes(name) || 
                                code === regionKey
                            );
                        if (matchingRegion) {
                            regionKey = matchingRegion[1]; // Usar el código
                        }
                    }

                    fillColor = CONFIG.REGION_COLORS[regionKey] || CONFIG.REGION_COLORS['Default'];
                    
                    if (fillColor === CONFIG.REGION_COLORS['Default']) {
                        debug(`No se encontró color para la región: ${countryData.region} (normalizada a ${regionKey})`);
                    }
                }
                break;
                
            case 'income':
                if (countryData.income) {
                    const incomeMappings = {
                        'High income': 'HIC',
                        'Upper middle income': 'UMC',
                        'Lower middle income': 'LMC',
                        'Low income': 'LIC'
                    };

                    let incomeKey = countryData.income;
                    if (incomeMappings[incomeKey]) {
                        incomeKey = incomeMappings[incomeKey];
                    }

                    fillColor = CONFIG.INCOME_GROUP_COLORS[incomeKey] || CONFIG.INCOME_GROUP_COLORS['Default'];
                    
                    if (fillColor === CONFIG.INCOME_GROUP_COLORS['Default']) {
                        debug(`No se encontró color para el grupo de ingresos: ${countryData.income}`);
                    }
                }
                break;
        }
    }
    
    return {
        fillColor: fillColor,
        weight: 1,
        opacity: 1,
        color: 'white',
        fillOpacity: fillOpacity
    };
}

/**
 * Resalta un país al pasar el cursor sobre él
 * @param {Object} e - Evento de Leaflet
 */
function highlightFeature(e) {
    const layer = e.target;
    
    if (layer !== selectedCountryLayer) {
        layer.setStyle({
            weight: 2.5,
            color: '#4b5563',
            dashArray: '',
            fillOpacity: layer.options.fillOpacity + 0.15
        });
    }
    
    if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
        layer.bringToFront();
    }
    
    if (selectedCountryLayer && selectedCountryLayer !== layer) {
        selectedCountryLayer.bringToFront();
    }
    
    // Mostrar tooltip con información básica
    const isoA3 = layer.feature.properties[GEOJSON_ISO_PROPERTY];
    const countryData = wasteDataCountries.find(c => c.iso3 === isoA3);
    
    if (countryData) {
        let tooltipContent = `<strong>${countryData.country_name}</strong>`;
        
        if (countryData.data_quality_score !== null && countryData.data_quality_score !== undefined) {
            const qualityClass = getQualityScoreClass(countryData.data_quality_score);
            const qualityText = qualityClass === 'score-high' ? 'Alta' : 
                               qualityClass === 'score-medium' ? 'Media' : 'Baja';
            
            tooltipContent += `<br>Calidad de datos: <span class="${qualityClass}">${qualityText} (${parseFloat(countryData.data_quality_score).toFixed(1)})</span>`;
        }
        
        if (countryData.cities?.length > 0) {
            tooltipContent += `<br>${countryData.cities.length} ciudades con datos`;
        }
        
        showTooltip(e, tooltipContent);
    }
}

/**
 * Quita el resaltado de un país
 * @param {Object} e - Evento de Leaflet
 */
function resetHighlight(e) {
    if (e.target !== selectedCountryLayer) {
        if (geoJsonCountriesLayer) geoJsonCountriesLayer.resetStyle(e.target);
    }
    
    hideTooltip();
}

/**
 * Maneja el clic en un país
 * @param {Object} e - Evento de Leaflet
 */
function onCountryClick(e) {
    const layer = e.target;
    const isoA3 = layer.feature.properties[GEOJSON_ISO_PROPERTY];
    
    if (selectedCountryLayer && selectedCountryLayer !== layer) {
        if (geoJsonCountriesLayer) geoJsonCountriesLayer.resetStyle(selectedCountryLayer);
    }
    
    layer.setStyle({
        fillColor: '#1d4ed8',
        fillOpacity: 0.85,
        weight: 2.5,
        color: '#333'
    });
    
    selectedCountryLayer = layer;
    layer.bringToFront();
    displayCityMarkers(isoA3);
}

/**
 * Configura propiedades para cada país en el mapa
 * @param {Object} feature - Feature GeoJSON
 * @param {Object} layer - Capa Leaflet
 */
function onEachFeature(feature, layer) {
    if (!feature.properties) return;
    
    const isoA3 = feature.properties[GEOJSON_ISO_PROPERTY];
    const countryNameGeo = feature.properties.ADMIN || feature.properties.NAME || feature.properties.name || 'País Desconocido';
    
    if (!isoA3) {
        layer.bindPopup(`<strong>${countryNameGeo}</strong><br>(ISO no encontrado en GeoJSON)`);
        return;
    }

    const countryData = wasteDataCountries.find(c => c.iso3 === isoA3);
    let popupContent = `<div class="p-1">
        <h3 class="text-sky-800 font-semibold text-sm mb-2">${countryData?.country_name || countryNameGeo} (${isoA3})</h3>`;
    
    if (countryData) {
        // Añadir información de calidad si está disponible
        if (countryData.data_quality_score !== null && countryData.data_quality_score !== undefined) {
            const qualityClass = getQualityScoreClass(countryData.data_quality_score);
            popupContent += `<div class="mb-2 text-xs">
                <span class="font-medium">Calidad de datos:</span> 
                <span class="${qualityClass === 'score-high' ? 'text-emerald-600' : 
                              qualityClass === 'score-medium' ? 'text-amber-600' : 'text-red-600'}">
                    ${parseFloat(countryData.data_quality_score).toFixed(1)}/100
                </span>
            </div>`;
        }
        
        popupContent += `<a href="country_view.html?iso3=${isoA3}" class="text-sky-600 hover:underline text-xs inline-block px-3 py-1 bg-sky-50 rounded-full mt-1">
            Ver detalles del país 
            <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 inline" viewBox="0 0 20 20" fill="currentColor">
                <path fill-rule="evenodd" d="M10.293 5.293a1 1 0 011.414 0l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414-1.414L12.586 11H5a1 1 0 110-2h7.586l-2.293-2.293a1 1 0 010-1.414z" clip-rule="evenodd" />
            </svg>
        </a>`;
        
        if (countryData.cities?.length > 0) {
            popupContent += `<div class="text-xs text-slate-500 mt-2 pt-2 border-t border-slate-200">
                ${countryData.cities.length} ciudad(es) con datos. 
                <span class="italic">Haz clic en el país para ver marcadores.</span>
            </div>`;
        }
    } else {
        popupContent += `<div class="text-slate-500 text-xs mt-1">Datos de gestión no disponibles en el índice</div>`;
    }
    
    popupContent += `</div>`;
    
    layer.bindPopup(popupContent);
    
    layer.on({
        mouseover: highlightFeature,
        mouseout: resetHighlight,
        click: onCountryClick
    });
}

/**
 * Muestra los marcadores de ciudades para un país específico
 * @param {string} countryIso3 - Código ISO3 del país
 */
function displayCityMarkers(countryIso3) {
    if (!map || !cityMarkersLayer) return;
    
    cityMarkersLayer.clearLayers();
    
    const countryData = wasteDataCountries.find(c => c.iso3 === countryIso3);
    if (!countryData?.cities?.length) return;
    
    debug(`Mostrando marcadores para ${countryData.cities.length} ciudades de ${countryData.country_name}`);
    
    let citiesWithCoords = 0;
    const cityBounds = L.latLngBounds();
    
    // Crear íconos de marcador personalizados
    const defaultIcon = L.divIcon({
        html: `<div class="w-3 h-3 bg-sky-500 rounded-full border-2 border-white shadow-md"></div>`,
        className: 'city-marker-icon',
        iconSize: [12, 12],
        iconAnchor: [6, 6]
    });
    
    const largeIcon = L.divIcon({
        html: `<div class="w-4 h-4 bg-amber-500 rounded-full border-2 border-white shadow-md"></div>`,
        className: 'city-marker-icon-large',
        iconSize: [16, 16],
        iconAnchor: [8, 8]
    });
    
    const capitalIcon = L.divIcon({
        html: `<div class="w-5 h-5 bg-rose-500 rounded-full border-2 border-white shadow-md flex items-center justify-center">
            <div class="w-2 h-2 bg-white rounded-full"></div>
        </div>`,
        className: 'city-marker-icon-capital',
        iconSize: [20, 20],
        iconAnchor: [10, 10]
    });
    
    countryData.cities.forEach(city => {
        const lat = parseFloat(city.latitude);
        const lon = parseFloat(city.longitude);
        
        if (!isNaN(lat) && !isNaN(lon) &&
            city.latitude_geo_status === 'found' &&
            city.longitude_geo_status === 'found') {
            
            citiesWithCoords++;
            
            // Elegir ícono basado en tipo de ciudad
            let cityIcon = defaultIcon;
            if (city.is_capital) {
                cityIcon = capitalIcon;
            } else if (city.population > 1000000) {
                cityIcon = largeIcon;
            }
            
            const cityMarker = L.marker([lat, lon], { icon: cityIcon });
            
            // Crear popup con información detallada
            let cityPopupContent = `<div class="p-1">
                <h3 class="text-sky-800 font-semibold text-sm mb-1">${city.name}</h3>`;
                
            if (city.is_capital) {
                cityPopupContent += `<div class="text-xs bg-rose-50 text-rose-600 px-2 py-0.5 rounded-full inline-block mb-2">Capital</div>`;
            }
            
            if (city.data_quality_score !== null && city.data_quality_score !== undefined) {
                const qualityClass = getQualityScoreClass(city.data_quality_score);
                cityPopupContent += `<div class="mb-2 text-xs">
                    <span class="font-medium">Calidad de datos:</span> 
                    <span class="${qualityClass === 'score-high' ? 'text-emerald-600' : 
                                  qualityClass === 'score-medium' ? 'text-amber-600' : 'text-red-600'}">
                        ${parseFloat(city.data_quality_score).toFixed(1)}/100
                    </span>
                </div>`;
            }
            
            cityPopupContent += `<a href="city_view.html?id=${city.id}&iso3=${countryIso3}" class="text-sky-600 hover:underline text-xs inline-block px-3 py-1 bg-sky-50 rounded-full mt-1">
                Ver detalles 
                <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 inline" viewBox="0 0 20 20" fill="currentColor">
                    <path fill-rule="evenodd" d="M10.293 5.293a1 1 0 011.414 0l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414-1.414L12.586 11H5a1 1 0 110-2h7.586l-2.293-2.293a1 1 0 010-1.414z" clip-rule="evenodd" />
                </svg>
            </a>
            </div>`;
            
            cityMarker.bindPopup(cityPopupContent);
            
            // Añadir tooltip para información rápida
            cityMarker.bindTooltip(city.name, {
                direction: 'top',
                offset: [0, -10],
                className: 'leaflet-tooltip-custom'
            });
            
            cityMarkersLayer.addLayer(cityMarker);
            cityBounds.extend([lat, lon]);
        }
    });
    
    debug(`${citiesWithCoords} ciudades con coordenadas válidas`);
    
    if (citiesWithCoords > 0) {
        try {
            if (selectedCountryLayer) {
                map.fitBounds(selectedCountryLayer.getBounds().extend(cityBounds), {
                    padding: [50,50],
                    maxZoom: 10,
                    animate: true,
                    duration: 0.5
                });
            } else {
                map.fitBounds(cityBounds, {
                    padding: [50,50],
                    maxZoom: 10,
                    animate: true,
                    duration: 0.5
                });
            }
        } catch (e) {
            console.warn("No se pudo hacer zoom a las ciudades:", e);
        }
    }
}

/**
 * Actualiza la visualización del mapa con países filtrados
 * @param {Array} countriesMatchingFilters - Lista de países que cumplen los filtros
 */
function updateMapVisualization(countriesMatchingFilters) {
    if (!map || !countryGeoJsonData) return;
    
    if (geoJsonCountriesLayer) {
        map.removeLayer(geoJsonCountriesLayer);
        geoJsonCountriesLayer = null;
    }
    
    const mapColorSelect = document.getElementById('map-color-select');
    if (mapColorSelect) {
        updateMapLegend(mapColorSelect.value);
    }
    
    const iso3InFilteredData = new Set(countriesMatchingFilters.map(c => c.iso3));
    
    const filteredGeoJsonFeatures = {
        type: "FeatureCollection",
        features: countryGeoJsonData.features.filter(feature =>
            feature.properties && iso3InFilteredData.has(feature.properties[GEOJSON_ISO_PROPERTY])
        )
    };
    
    debug(`GeoJSON filtrado: ${filteredGeoJsonFeatures.features.length} países de ${countryGeoJsonData.features.length} totales`);
    
    if (filteredGeoJsonFeatures.features.length > 0) {
        geoJsonCountriesLayer = L.geoJSON(filteredGeoJsonFeatures, {
            style: styleCountry,
            onEachFeature: onEachFeature
        }).addTo(map);
    }
    
    if (cityMarkersLayer) cityMarkersLayer.clearLayers();
    selectedCountryLayer = null;
    
    const loadingMapElement = document.getElementById('loading-map');
    if (loadingMapElement) loadingMapElement.style.display = 'none';
}