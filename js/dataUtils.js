/**
 * Funciones para el manejo de datos
 */

// Variables globales para datos (definidas en main.js o config.js y pasadas si es necesario)
// let wasteDataCountries = [];
// let allCitiesFlatList = [];
// let countryGeoJsonData = null;

/**
 * Carga los datos iniciales desde archivos JSON y GeoJSON
 * @returns {Promise<Object>} Una promesa que se resuelve con los datos cargados
 */
async function loadInitialData() {
    try {
        debug("Iniciando carga de datos...");

        const [geoData, indexJsonData] = await Promise.all([
            fetch(`${UI_DATA_DIR}countries_polygons.geojson`)
                .then(response => {
                    if (!response.ok) throw new Error(`HTTP ${response.status} cargando GeoJSON desde ${UI_DATA_DIR}countries_polygons.geojson`);
                    return response.json();
                }),
            fetch(`${UI_DATA_DIR}index.json`)
                .then(response => {
                    if (!response.ok) throw new Error(`HTTP ${response.status} cargando index.json desde ${UI_DATA_DIR}`);
                    return response.json();
                })
        ]);

        countryGeoJsonData = geoData;
        wasteDataCountries = indexJsonData.countries || [];

        debug(`Datos cargados: ${wasteDataCountries.length} países en wasteDataCountries`);

        processAllCities();

        return {
            regions: indexJsonData.regions || [],
            incomeGroups: indexJsonData.income_groups || [],
            summary_by_income_group: indexJsonData.summary_by_income_group || [],
            summary_by_region: indexJsonData.summary_by_region || []
        };
    } catch (error) {
        console.error("Error cargando datos iniciales:", error);
        throw error;
    }
}

/**
 * Actualiza la tabla de los 10 principales generadores de residuos dinámicamente.
 * MODIFICADO: Añade PIB Total y Residuos Per Cápita.
 * @param {Array} filteredCountriesData - Países que cumplen con los filtros actuales
 */
function updateTopWasteGeneratorsTable(filteredCountriesData) {
    const tableBody = document.getElementById('top-waste-generators-body');
    if (!tableBody) {
        debug("Elemento top-waste-generators-body no encontrado.");
        return;
    }

    const topCountries = filteredCountriesData
        .filter(country => {
            const totalWaste = parseFloat(country.total_msw_total_tonnes);
            return !isNaN(totalWaste) && totalWaste > 0;
        })
        .sort((a, b) => parseFloat(b.total_msw_total_tonnes) - parseFloat(a.total_msw_total_tonnes))
        .slice(0, 10);

    debug(`Encontrados ${topCountries.length} países para la tabla top 10 (después de aplicar filtros).`);

    tableBody.innerHTML = '';
    if (topCountries.length > 0) {
        tableBody.innerHTML = topCountries.map(country => {
            const totalWaste = parseFloat(country.total_msw_total_tonnes);
            const population = parseFloat(country.population_total);
            const gdpPerCapita = parseFloat(country.gdp_per_capita_usd);
            const gdpTotal = parseFloat(country.gdp_total_usd); // Campo de index.json
            const wastePerCapita = parseFloat(country.waste_per_capita_kg_day); // Campo de index.json
            const recyclingRate = parseFloat(country.waste_treatment_recycling_percent);
            const collectionCoverage = parseFloat(country.waste_collection_coverage_total_percent_of_population);
            const qualityScore = parseFloat(country.data_quality_score);

            return `
                <tr class="hover:bg-slate-50 transition-colors">
                    <td class="px-4 py-3 border-b border-slate-200 text-sm">
                        <div class="flex items-center">
                            <span class="font-medium text-slate-700">${country.country_name}</span>
                            <span class="ml-2 text-xs text-slate-500">(${country.iso3})</span>
                        </div>
                    </td>
                    <td class="px-4 py-3 border-b border-slate-200 text-right font-medium text-sm">
                        ${!isNaN(totalWaste) ? totalWaste.toLocaleString('es-ES', {maximumFractionDigits: 0}) : 'N/A'}
                    </td>
                    <td class="px-4 py-3 border-b border-slate-200 text-right text-sm">
                        ${!isNaN(population) ? population.toLocaleString('es-ES', {maximumFractionDigits: 0}) : 'N/A'}
                    </td>
                    <td class="px-4 py-3 border-b border-slate-200 text-right text-sm">
                        ${!isNaN(gdpPerCapita) ? gdpPerCapita.toLocaleString('es-ES', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }) : 'N/A'}
                    </td>
                    <td class="px-4 py-3 border-b border-slate-200 text-right text-sm">
                        ${!isNaN(gdpTotal) ? gdpTotal.toLocaleString('es-ES', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }) : 'N/A'}
                    </td>
                    <td class="px-4 py-3 border-b border-slate-200 text-right text-sm">
                        ${!isNaN(wastePerCapita) ? wastePerCapita.toFixed(2) : 'N/A'}
                    </td>
                    <td class="px-4 py-3 border-b border-slate-200 text-right text-sm">
                        ${!isNaN(recyclingRate) ? recyclingRate.toFixed(1) + '%' : 'N/A'}
                    </td>
                    <td class="px-4 py-3 border-b border-slate-200 text-right text-sm">
                        ${!isNaN(collectionCoverage) ? collectionCoverage.toFixed(1) + '%' : 'N/A'}
                    </td>
                    <td class="px-4 py-3 border-b border-slate-200 text-right text-sm">
                        ${!isNaN(qualityScore) ? qualityScore.toFixed(1) : 'N/A'}
                    </td>
                </tr>
            `;
        }).join('');
    } else {
        tableBody.innerHTML = `
            <tr>
                <td colspan="9" class="px-4 py-8 text-center text-slate-500 italic"> 
                    No hay datos suficientes para mostrar los principales generadores de residuos con los filtros actuales.
                </td>
            </tr>
        `;
    }
}


/**
 * Procesa todas las ciudades de todos los países en una lista plana
 */
function processAllCities() {
    allCitiesFlatList = [];
    debug("Procesando ciudades de todos los países...");

    let cityCount = 0;
    if (wasteDataCountries && Array.isArray(wasteDataCountries)) {
        wasteDataCountries.forEach(country => {
            if (country.cities && Array.isArray(country.cities)) {
                country.cities.forEach(city => {
                    allCitiesFlatList.push({
                        ...city,
                        country_iso3: country.iso3,
                        country_name: country.country_name,
                        region: country.region,
                        income: country.income,
                        data_quality_score: city.data_quality_score !== undefined ? city.data_quality_score : null
                    });
                });
                cityCount += country.cities.length;
            }
        });
    }
    debug(`Procesadas ${cityCount} ciudades en total y almacenadas en allCitiesFlatList.`);
}

/**
 * Rellena un elemento <select> con opciones
 * @param {HTMLElement} selectElement - El elemento select a rellenar
 * @param {Array<string>} options - Array de opciones para el select
 */
function populateSelect(selectElement, options) {
    if (!selectElement) {
        debug("Elemento select no encontrado para populateSelect");
        return;
    }

    const firstOptionValue = selectElement.options[0] ? selectElement.options[0].value : "";
    const firstOptionText = selectElement.options[0] ? selectElement.options[0].textContent : "Seleccionar...";

    selectElement.innerHTML = '';

    const defaultOpt = document.createElement('option');
    defaultOpt.value = firstOptionValue;
    defaultOpt.textContent = firstOptionText;
    selectElement.appendChild(defaultOpt);

    if (options && Array.isArray(options)) {
        options.sort((a, b) => String(a).localeCompare(String(b)));
        options.forEach(optionValue => {
            if (optionValue) {
                const option = document.createElement('option');
                option.value = optionValue;
                option.textContent = (CONFIG.REGION_DISPLAY_NAMES && CONFIG.REGION_DISPLAY_NAMES[optionValue]) || optionValue;
                selectElement.appendChild(option);
            }
        });
    }
}

/**
 * Obtiene la clase CSS para un puntaje de calidad
 * @param {number | null | undefined} score - Puntaje de calidad
 * @returns {string} Nombre de clase CSS
 */
function getQualityScoreClass(score) {
    if (score === null || score === undefined || isNaN(parseFloat(score))) return '';
    const numericScore = parseFloat(score);
    if (numericScore >= 75) return 'score-high';
    if (numericScore >= 50) return 'score-medium';
    return 'score-low';
}

/**
 * Aplica filtros a la lista de entidades y actualiza la visualización.
 * @param {string} currentView - Vista actual ('countries' o 'cities')
 * @param {Object} filters - Objeto con los filtros a aplicar
 * @returns {Object} Objeto con los resultados filtrados: { groupedEntities, countriesMatchingFilters }
 */
function applyFilters(currentView, filters) {
    const { searchTerm, selectedRegion, selectedIncome, sortValue } = filters;

    debug(`Aplicando filtros: vista=${currentView}, búsqueda="${searchTerm}", región=${selectedRegion}, ingresos=${selectedIncome}, orden=${sortValue}`);

    let baseEntities = [];
    if (currentView === 'countries') {
        baseEntities = wasteDataCountries;
    } else {
        baseEntities = allCitiesFlatList;
    }

    let filteredEntitiesList = baseEntities.filter(entity => {
        const nameField = currentView === 'countries' ? entity.country_name : entity.name;
        const isoField = currentView === 'countries' ? entity.iso3 : entity.country_iso3;
        const countryNameForCitySearch = currentView === 'cities' ? entity.country_name : '';

        const nameMatch = nameField?.toLowerCase().includes(searchTerm) ||
                          (currentView === 'cities' && countryNameForCitySearch?.toLowerCase().includes(searchTerm));
        const isoMatch = isoField?.toLowerCase().includes(searchTerm);

        const entityRegion = entity.region;
        const entityIncome = entity.income;

        const regionMatch = !selectedRegion || entityRegion === selectedRegion;
        const incomeMatch = !selectedIncome || entityIncome === selectedIncome;

        return (nameMatch || isoMatch) && regionMatch && incomeMatch;
    });

    const sortKeyName = currentView === 'countries' ? 'country_name' : 'name';
    filteredEntitiesList.sort((a, b) => {
        if (sortValue === 'quality_desc') {
            return (b.data_quality_score ?? -1) - (a.data_quality_score ?? -1);
        } else if (sortValue === 'quality_asc') {
            return (a.data_quality_score ?? Infinity) - (b.data_quality_score ?? Infinity);
        } else {
            return (a[sortKeyName] || "").localeCompare(b[sortKeyName] || "");
        }
    });

    const groupedEntities = {};
    filteredEntitiesList.forEach(entity => {
        const regionKey = entity.region || 'Sin Región Especificada';
        if (!groupedEntities[regionKey]) {
            groupedEntities[regionKey] = [];
        }
        groupedEntities[regionKey].push(entity);
    });

    const sortedGroupedEntities = {};
    Object.keys(groupedEntities).sort((a, b) => {
        const displayA = (CONFIG.REGION_DISPLAY_NAMES && CONFIG.REGION_DISPLAY_NAMES[a]) || a;
        const displayB = (CONFIG.REGION_DISPLAY_NAMES && CONFIG.REGION_DISPLAY_NAMES[b]) || b;
        return displayA.localeCompare(displayB);
    }).forEach(regionNameKey => {
        sortedGroupedEntities[regionNameKey] = groupedEntities[regionNameKey];
    });

    debug(`Filtrado y agrupado completado: ${Object.keys(sortedGroupedEntities).length} regiones con entidades.`);

    const countriesMatchingFilters = wasteDataCountries.filter(country => {
        const regionMatch = !selectedRegion || country.region === selectedRegion;
        const incomeMatch = !selectedIncome || country.income === selectedIncome;
        let searchOverallMatch = true;
        if (searchTerm) {
            searchOverallMatch = country.country_name?.toLowerCase().includes(searchTerm) || country.iso3?.toLowerCase().includes(searchTerm);
            if (!searchOverallMatch && currentView === 'cities' && country.cities && country.cities.length > 0) {
                 searchOverallMatch = filteredEntitiesList.some(city => city.country_iso3 === country.iso3);
            }
        }
        return searchOverallMatch && regionMatch && incomeMatch;
    });

    return {
        groupedEntities: sortedGroupedEntities,
        countriesMatchingFilters
    };
}


/**
 * Actualiza la visualización de la lista de entidades.
 * @param {Object} groupedEntities - Objeto con entidades agrupadas por región
 * @param {string} currentView - Vista actual ('countries' o 'cities')
 */
function updateDisplayedEntities(groupedEntities, currentView) {
    const entityListElement = document.getElementById('entity-list');
    const noResultsElement = document.getElementById('no-results');
    const loadingListElement = document.getElementById('loading-list');
    const entityListTitleElement = document.getElementById('entity-list-title');

    if (!entityListElement) {
        debug("Elemento entity-list no encontrado.");
        return;
    }

    entityListElement.innerHTML = '';

    if (entityListTitleElement) {
        const baseTitle = currentView === 'countries' ? "Lista de Países" : "Lista de Ciudades";
        entityListTitleElement.innerHTML = `
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 mr-2 text-sky-600" viewBox="0 0 20 20" fill="currentColor">
              <path fill-rule="evenodd" d="M3 4a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zm0 4a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zm0 4a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zm0 4a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1z" clip-rule="evenodd" />
            </svg>
            ${baseTitle}
        `;
    }

    const regionKeys = Object.keys(groupedEntities);

    if (regionKeys.length > 0) {
        if (noResultsElement) noResultsElement.classList.add('hidden');

        regionKeys.forEach(regionKeyName => {
            const regionHeaderItem = document.createElement('li');
            regionHeaderItem.className = 'col-span-1 md:col-span-2 lg:col-span-3 mt-4 mb-2 pb-1 border-b-2 border-sky-200';

            const regionTitle = document.createElement('h3');
            regionTitle.className = 'text-lg font-semibold text-sky-700';
            const displayRegionName = (CONFIG.REGION_DISPLAY_NAMES && CONFIG.REGION_DISPLAY_NAMES[regionKeyName]) || regionKeyName;
            regionTitle.textContent = displayRegionName;

            regionHeaderItem.appendChild(regionTitle);
            entityListElement.appendChild(regionHeaderItem);

            groupedEntities[regionKeyName].forEach(entity => {
                const listItem = document.createElement('li');
                listItem.className = 'list-item bg-white p-4 rounded-lg shadow hover:shadow-md transition-shadow duration-200 ease-in-out flex justify-between items-center';

                const linkWrapper = document.createElement('div');
                linkWrapper.className = 'flex-grow mr-3 overflow-hidden';

                const link = document.createElement('a');
                link.className = 'text-sky-700 hover:text-sky-900 font-semibold text-base truncate block';

                const detailText = document.createElement('span');
                detailText.className = 'text-xs text-slate-500 block mt-0.5 truncate';

                if (currentView === 'countries') {
                    link.href = `country_view.html?iso3=${entity.iso3}`;
                    link.textContent = `${entity.country_name}`;
                    detailText.textContent = `(${entity.iso3}) - Ingresos: ${entity.income || 'N/A'}`;
                } else {
                    link.href = `city_view.html?id=${entity.id}&iso3=${entity.country_iso3}`;
                    link.textContent = `${entity.name}`;
                    detailText.textContent = `(${entity.country_name}, ${entity.country_iso3})`;
                }

                linkWrapper.appendChild(link);
                linkWrapper.appendChild(detailText);
                listItem.appendChild(linkWrapper);

                if (entity.data_quality_score !== null && entity.data_quality_score !== undefined) {
                    const scoreWrapper = document.createElement('div');
                    scoreWrapper.className = 'ml-auto text-right flex-shrink-0';

                    const scoreSpan = document.createElement('span');
                    scoreSpan.textContent = `${parseFloat(entity.data_quality_score).toFixed(1)}`;
                    scoreSpan.className = `quality-score ${getQualityScoreClass(entity.data_quality_score)} text-sm font-bold px-2 py-1 rounded`;

                    const scoreLabel = document.createElement('span');
                    scoreLabel.textContent = 'Calidad';
                    scoreLabel.className = 'block text-xs text-slate-500 mt-0.5';

                    scoreWrapper.appendChild(scoreSpan);
                    scoreWrapper.appendChild(scoreLabel);
                    listItem.appendChild(scoreWrapper);
                }
                entityListElement.appendChild(listItem);
            });
        });
    } else {
        if (noResultsElement) noResultsElement.classList.remove('hidden');
    }

    if (loadingListElement) loadingListElement.style.display = 'none';
}


/**
 * Puebla una tabla de resumen con datos agregados.
 * @param {string} tableBodyId - ID del tbody de la tabla.
 * @param {Array<Object>} summaryData - Array de objetos, cada uno representando una fila.
 * @param {string} groupKey - La clave en cada objeto de summaryData que contiene el nombre del grupo (ej. 'income_group').
 * @param {Array<Object>} metrics - Array de objetos definiendo las métricas a mostrar.
 * @param {string} [groupLabelKey=groupKey] - Clave opcional para usar como etiqueta del grupo si es diferente de groupKey.
 */
function populateSummaryTable(tableBodyId, summaryData, groupKey, metrics, groupLabelKey) {
    const tableBody = document.getElementById(tableBodyId);
    const groupDisplayKey = groupLabelKey || groupKey;

    if (!tableBody) {
        debug(`Elemento de tabla con ID '${tableBodyId}' no encontrado.`);
        return;
    }

    tableBody.innerHTML = '';

    if (!summaryData || summaryData.length === 0) {
        const colspan = metrics.length + 2;
        tableBody.innerHTML = `<tr><td colspan="${colspan}" class="text-center text-slate-500 py-4 px-4 text-sm italic">No hay datos de resumen disponibles.</td></tr>`;
        debug(`No hay datos de resumen para la tabla ${tableBodyId}`);
        return;
    }

    debug(`Poblando tabla ${tableBodyId} con ${summaryData.length} grupos.`);

    summaryData.forEach(groupStats => {
        const row = tableBody.insertRow();
        row.className = "hover:bg-slate-50 transition-colors";

        let cell = row.insertCell();
        const rawGroupValue = groupStats[groupDisplayKey];
        let displayGroupValue = rawGroupValue;
        if (groupKey === 'region' && CONFIG.REGION_DISPLAY_NAMES && CONFIG.REGION_DISPLAY_NAMES[rawGroupValue]) {
            displayGroupValue = CONFIG.REGION_DISPLAY_NAMES[rawGroupValue];
        } else if (groupKey === 'income_group' && CONFIG.INCOME_GROUP_DISPLAY_NAMES && CONFIG.INCOME_GROUP_DISPLAY_NAMES[rawGroupValue]) { // Suponiendo que INCOME_GROUP_DISPLAY_NAMES podría existir
            displayGroupValue = CONFIG.INCOME_GROUP_DISPLAY_NAMES[rawGroupValue];
        }
        cell.textContent = displayGroupValue || 'N/A';
        cell.className = 'px-4 py-3 border-b border-slate-200 text-sm text-slate-700 font-medium';

        cell = row.insertCell();
        cell.textContent = groupStats.country_count ? groupStats.country_count.toLocaleString('es-ES') : '0';
        cell.className = 'px-4 py-3 border-b border-slate-200 text-sm text-slate-600 text-right';

        metrics.forEach(metricInfo => {
            cell = row.insertCell();
            const value = groupStats[metricInfo.key];
            cell.textContent = (value !== null && value !== undefined && !isNaN(parseFloat(value)))
                                ? parseFloat(value).toFixed(metricInfo.decimals !== undefined ? metricInfo.decimals : 1) + (metricInfo.unit || '')
                                : 'N/A';
            cell.className = 'px-4 py-3 border-b border-slate-200 text-sm text-slate-600 text-right';
        });
    });
}
