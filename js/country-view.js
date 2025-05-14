// Funciones auxiliares
function getQualityScoreClass(score) {
    if (score === null || score === undefined || isNaN(parseFloat(score))) return '';
    const numericScore = parseFloat(score);
    if (numericScore >= 75) return 'score-high';
    if (numericScore >= 50) return 'score-medium';
    return 'score-low';
}

function displayDataWithStatus(dataObject, dataKey, elementId, unit = '', decimals = 2) {
    const value = dataObject ? dataObject[dataKey] : undefined;
    let statusKey = `${dataKey}_status`;
    
    // Manejar casos especiales de claves de estado
    if (dataObject && !dataObject.hasOwnProperty(statusKey)) {
        const baseKey = dataKey.replace(/_percent$/, '').replace(/_tons_year$/, '').replace(/_usd$/, '');
        if (dataKey === 'country_code_iso3' && dataObject.hasOwnProperty('country_code_iso3_status')) {
            statusKey = 'country_code_iso3_status';
        } else if (dataKey === 'iso3c' && dataObject.hasOwnProperty('iso3c_status')) {
            statusKey = 'iso3c_status';
        } else if (dataObject.hasOwnProperty(`${baseKey}_status`)) {
            statusKey = `${baseKey}_status`;
        }
    }
    
    const statusValue = dataObject ? dataObject[statusKey] : undefined;
    const valueElement = document.getElementById(elementId);
    const statusElement = document.getElementById(`${elementId}-status`);

    if (valueElement) {
        valueElement.className = 'data-value';
        if (value !== null && value !== undefined && String(value).toLowerCase() !== 'nan' && String(value).trim() !== '') {
            if (typeof value === 'number') {
                if (elementId === 'country-quality-score') {
                    valueElement.textContent = parseFloat(value).toFixed(1);
                    valueElement.className = `quality-score-display ${getQualityScoreClass(value)}`;
                } else if (Math.abs(value) >= 1000 || unit === '%' || Number.isInteger(value)) {
                    valueElement.textContent = value.toLocaleString('es-ES', { maximumFractionDigits: decimals }) + unit;
                } else {
                    valueElement.textContent = parseFloat(value.toFixed(decimals)) + unit;
                }
            } else {
                valueElement.textContent = String(value) + unit;
            }
        } else {
            valueElement.textContent = 'N/A';
        }
    }

    if (statusElement) {
        if (statusValue && String(statusValue).toLowerCase() !== 'nan') {
            statusElement.textContent = String(statusValue).replace(/_/g, ' ');
            statusElement.className = 'data-status';
            statusElement.classList.add(`status-${String(statusValue).toLowerCase().replace(/\+/g, '_')}`);
        } else {
            statusElement.textContent = '';
            statusElement.className = 'data-status';
        }
    }
}

let treatmentChartInstance = null;
function createTreatmentChart(countryData) {
    const ctx = document.getElementById('treatmentChart')?.getContext('2d');
    const noDataElement = document.getElementById('no-treatment-data');
    
    if (!ctx || !noDataElement) {
        if (noDataElement) noDataElement.style.display = 'block';
        if (ctx && ctx.canvas.parentNode) ctx.canvas.parentNode.style.display = 'none';
        return;
    }

    const treatmentData = [
        { key: 'waste_treatment_recycling_percent', label: 'Reciclaje', color: '#34D399' },
        { key: 'waste_treatment_compost_percent', label: 'Compostaje', color: '#FBBF24' },
        { key: 'waste_treatment_incineration_percent', label: 'Incineración', color: '#F87171' },
        { key: 'waste_treatment_sanitary_landfill_percent', label: 'Vertedero Sanitario', color: '#A5B4FC' },
        { key: 'waste_treatment_controlled_landfill_percent', label: 'Vertedero Controlado', color: '#7DD3FC' },
        { key: 'waste_treatment_landfill_unspecified_percent', label: 'Vertedero No Especificado', color: '#9CA3AF' },
        { key: 'waste_treatment_open_dump_percent', label: 'Vertedero Abierto', color: '#EF4444' }
    ];

    const filteredData = treatmentData
        .map(item => ({
            ...item,
            value: parseFloat(countryData[item.key]) || 0
        }))
        .filter(item => item.value > 0.01);

    if (filteredData.length > 0) {
        noDataElement.style.display = 'none';
        if (ctx.canvas.parentNode) ctx.canvas.parentNode.style.display = 'block';
        
        if (treatmentChartInstance) {
            treatmentChartInstance.destroy();
        }

        treatmentChartInstance = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: filteredData.map(item => item.label),
                datasets: [{
                    label: 'Tratamiento (%)',
                    data: filteredData.map(item => item.value),
                    backgroundColor: filteredData.map(item => item.color),
                    borderColor: '#fff',
                    borderWidth: 2,
                    hoverOffset: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 15,
                            boxWidth: 12,
                            font: {
                                family: "'Inter', sans-serif"
                            }
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: context => `${context.label}: ${context.parsed.toFixed(1)}%`
                        }
                    }
                },
                cutout: '60%'
            }
        });
    } else {
        noDataElement.style.display = 'block';
        if (ctx.canvas.parentNode) ctx.canvas.parentNode.style.display = 'none';
    }
}

function createPlotlyCompositionChart(countryData) {
    const containerId = 'compositionPlotlyChartContainer';
    const noDataMsgElement = document.getElementById('no-composition-data-plotly');
    const chartContainer = document.getElementById(containerId);

    if (!chartContainer || !noDataMsgElement) return;

    const compositionData = [
        { key: 'composition_food_organic', label: 'Orgánico/Comida', color: '#8BC34A' },
        { key: 'composition_paper_cardboard', label: 'Papel/Cartón', color: '#FFC107' },
        { key: 'composition_plastic', label: 'Plástico', color: '#2196F3' },
        { key: 'composition_glass', label: 'Vidrio', color: '#9E9E9E' },
        { key: 'composition_metal', label: 'Metal', color: '#FF5722' },
        { key: 'composition_wood', label: 'Madera', color: '#795548' },
        { key: 'composition_rubber_leather', label: 'Caucho/Cuero', color: '#607D8B' },
        { key: 'composition_yard_garden_green', label: 'Residuos Verdes', color: '#4CAF50' },
        { key: 'composition_other', label: 'Otros', color: '#F44336' }
    ];

    const filteredData = compositionData
        .map(item => ({
            ...item,
            value: parseFloat(countryData[item.key]) || 0
        }))
        .filter(item => item.value > 0);

    if (filteredData.length > 0) {
        noDataMsgElement.style.display = 'none';
        chartContainer.style.display = 'block';

        const data = [{
            values: filteredData.map(item => item.value),
            labels: filteredData.map(item => item.label),
            type: 'pie',
            hole: 0.4,
            textinfo: "label+percent",
            textposition: 'inside',
            insidetextorientation: 'radial',
            hoverinfo: 'label+percent+value',
            marker: {
                colors: filteredData.map(item => item.color)
            }
        }];

        const layout = {
            title: {
                text: 'Composición Detallada de Residuos (%)',
                font: { 
                    size: 16,
                    color: '#1e3a8a',
                    family: "'Inter', sans-serif"
                },
                y: 0.95
            },
            height: 450,
            showlegend: true,
            legend: {
                orientation: 'h',
                y: -0.2,
                x: 0.5,
                xanchor: 'center',
                font: {
                    family: "'Inter', sans-serif",
                    size: 11
                }
            },
            margin: { t: 60, b: 80, l: 40, r: 40 }
        };

        const config = {
            responsive: true,
            displayModeBar: false
        };

        Plotly.newPlot(containerId, data, layout, config);
    } else {
        noDataMsgElement.style.display = 'block';
        chartContainer.style.display = 'none';
    }
}

function displayMeasurementsData(measurements) {
    const measurementsListDiv = document.getElementById('measurements-list');
    const measurementsSectionDiv = document.getElementById('measurements-data-section');
    
    if (!measurements || measurements.length === 0) {
        measurementsListDiv.innerHTML = '<p class="no-data">No hay datos de mediciones adicionales disponibles para este país.</p>';
        measurementsSectionDiv.style.display = 'block';
        return;
    }

    measurementsSectionDiv.style.display = 'block';
    measurementsListDiv.innerHTML = '';

    measurements.forEach(item => {
        const measurementDiv = document.createElement('div');
        measurementDiv.classList.add('measurement-item');

        let content = `<h3 class="text-lg font-semibold text-sky-700 mb-2">${item.measurement || 'Medición Desconocida'}</h3>`;
        
        if (item.units && String(item.units).toLowerCase() !== 'nan') {
            content += `<p><span class="data-label">Unidades:</span> ${item.units}</p>`;
        }
        
        if (item.year && String(item.year).toLowerCase() !== 'nan') {
            content += `<p><span class="data-label">Año:</span> ${item.year}</p>`;
        }
        
        if (item.source && String(item.source).toLowerCase() !== 'nan') {
            let sourceText = item.source;
            if (sourceText.length > 150) {
                sourceText = sourceText.substring(0, 147) + "...";
            }
            if (item.source.startsWith('http://') || item.source.startsWith('https://')) {
                content += `<p><span class="data-label">Fuente:</span> <a href="${item.source}" target="_blank" rel="noopener noreferrer" class="text-sky-600 hover:underline">${sourceText}</a></p>`;
            } else {
                content += `<p><span class="data-label">Fuente:</span> ${sourceText}</p>`;
            }
        }
        
        if (item.comments && String(item.comments).toLowerCase() !== 'nan') {
            content += `<p class="mt-2"><span class="data-label">Comentarios:</span> <span class="text-gray-700">${item.comments}</span></p>`;
        }

        measurementDiv.innerHTML = content;
        measurementsListDiv.appendChild(measurementDiv);
    });
}

function displayAiProfileData(profileData) {
    const container = document.getElementById('ai-profile-container');
    if (!container) return;

    if (!profileData || Object.keys(profileData).length === 0) {
        container.innerHTML = '<p class="no-data">Perfil analítico IA no disponible para esta entidad.</p>';
        return;
    }

    const sectionTitles = {
        overall_summary: "Resumen General",
        generation_context: "Contexto de Generación de Residuos",
        waste_stream_composition: "Composición del Flujo de Residuos",
        collection_and_transport: "Recolección y Transporte",
        treatment_and_disposal: "Tratamiento y Disposición Final",
        recycling_and_recovery_initiatives: "Iniciativas de Reciclaje y Recuperación",
        policy_and_governance: "Política y Gobernanza",
        overall_assessment: "Evaluación General"
    };

    const mainSectionOrder = [
        "overall_summary",
        "generation_context",
        "waste_stream_composition",
        "collection_and_transport",
        "treatment_and_disposal",
        "recycling_and_recovery_initiatives",
        "policy_and_governance",
        "overall_assessment"
    ];

    const subSectionTitles = {
        scale_and_rate: "Escala y Tasa de Generación",
        contributing_factors_trends: "Factores Contribuyentes y Tendencias",
        summary: "Resumen de Composición",
        data_notes: "Notas sobre Datos de Composición",
        coverage_and_methods: "Cobertura y Métodos",
        key_challenges: "Desafíos Clave",
        dominant_methods_summary: "Resumen de Métodos Dominantes",
        infrastructure_highlights: "Infraestructura Destacada",
        rates_and_targets: "Tasas y Objetivos",
        programs_mentioned: "Programas Mencionados",
        informal_sector_role: "Rol del Sector Informal",
        regulatory_framework: "Marco Regulatorio",
        governance_issues: "Problemas de Gobernanza",
        strengths: "Fortalezas",
        weaknesses_or_challenges: "Debilidades o Desafíos",
        recent_developments_or_outlook: "Desarrollos Recientes o Perspectivas"
    };

    let htmlContent = '';

    for (const sectionKey of mainSectionOrder) {
        if (profileData.hasOwnProperty(sectionKey)) {
            const sectionData = profileData[sectionKey];
            const sectionTitle = sectionTitles[sectionKey] || sectionKey.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
            
            htmlContent += `<div class="ai-profile-subsection">
                <h3>${sectionTitle}</h3>
                <div class="ai-profile-content">`;

            if (typeof sectionData === 'string') {
                htmlContent += `<p>${sectionData.replace(/\n/g, '<br>')}</p>`;
            } else if (typeof sectionData === 'object' && sectionData !== null) {
                for (const subKey in sectionData) {
                    if (sectionData.hasOwnProperty(subKey)) {
                        const subData = sectionData[subKey];
                        const subTitle = subSectionTitles[subKey] || subKey.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                        
                        htmlContent += `<strong>${subTitle}:</strong> `;
                        
                        if (Array.isArray(subData)) {
                            if (subData.length > 0) {
                                htmlContent += `<ul>`;
                                subData.forEach(item => {
                                    htmlContent += `<li>${String(item).replace(/\n/g, '<br>')}</li>`;
                                });
                                htmlContent += `</ul>`;
                            } else {
                                htmlContent += `<p class="no-data">No disponible.</p>`;
                            }
                        } else if (typeof subData === 'string' && subData.trim() !== '') {
                            htmlContent += `<p>${subData.replace(/\n/g, '<br>')}</p>`;
                        } else {
                            htmlContent += `<p class="no-data">No disponible.</p>`;
                        }
                    }
                }
            }

            htmlContent += `</div></div>`;
        }
    }

    container.innerHTML = htmlContent;
}

// Inicialización cuando el DOM está completamente cargado
document.addEventListener('DOMContentLoaded', function() {
    const params = new URLSearchParams(window.location.search);
    const iso3 = params.get('iso3');
    
    const loadingElement = document.getElementById('loading');
    const errorElement = document.getElementById('error-message');
    const detailsElement = document.getElementById('country-details');
    const cityListElement = document.getElementById('city-list');
    const noCitiesElement = document.getElementById('no-cities');
    const scoreSummaryElement = document.getElementById('country-score-calculation-summary');

    const UI_DATA_DIR = './output/ui_data/';

    if (!iso3) {
        loadingElement.style.display = 'none';
        errorElement.textContent = 'Error: No se especificó el código ISO3 del país en la URL.';
        errorElement.classList.remove('hidden');
        detailsElement.classList.add('hidden');
        return;
    }

    const countryDataPath = `${UI_DATA_DIR}countries/${iso3.toUpperCase()}_country.json`;
    const indexDataPath = `${UI_DATA_DIR}index.json`;

    Promise.all([
        fetch(countryDataPath).then(res => {
            if (!res.ok) throw new Error(`Error ${res.status} cargando ${countryDataPath}`);
            return res.json();
        }),
        fetch(indexDataPath).then(res => {
            if (!res.ok) throw new Error(`Error ${res.status} cargando ${indexDataPath}`);
            return res.json();
        })
    ])
    .then(([countryData, indexData]) => {
        loadingElement.style.display = 'none';
        detailsElement.classList.remove('hidden');
        errorElement.classList.add('hidden');

        // Actualizar título de la página
        document.title = `${countryData.country_name || 'País'} - Detalles`;

        // Mostrar datos generales
        displayDataWithStatus(countryData, 'country_name', 'country-name-header');
        displayDataWithStatus(countryData, 'country_name', 'country-name');
        displayDataWithStatus(countryData, 'country_code_iso3', 'country-iso3');
        displayDataWithStatus(countryData, 'region', 'country-region');
        displayDataWithStatus(countryData, 'income_group_wb', 'country-income');
        displayDataWithStatus(countryData, 'population_total', 'country-population', '', 0);
        displayDataWithStatus(countryData, 'gdp_per_capita_usd', 'country-gdp', ' USD');
        displayDataWithStatus(countryData, 'data_quality_score', 'country-quality-score');

        // Mostrar datos de gestión de residuos
        displayDataWithStatus(countryData, 'total_msw_generated_tons_year', 'msw-generated', ' Ton/año');
        displayDataWithStatus(countryData, 'waste_collection_coverage_total_percent_of_population', 'collection-coverage', '%');
        displayDataWithStatus(countryData, 'waste_treatment_recycling_percent', 'treatment-recycling', '%');
        displayDataWithStatus(countryData, 'waste_treatment_compost_percent', 'treatment-compost', '%');
        displayDataWithStatus(countryData, 'waste_treatment_incineration_percent', 'treatment-incineration', '%');
        displayDataWithStatus(countryData, 'waste_treatment_sanitary_landfill_percent', 'treatment-sanitary-lf', '%');
        displayDataWithStatus(countryData, 'waste_treatment_controlled_landfill_percent', 'treatment-controlled-lf', '%');
        displayDataWithStatus(countryData, 'waste_treatment_landfill_unspecified_percent', 'treatment-unspecified-lf', '%');
        displayDataWithStatus(countryData, 'waste_treatment_open_dump_percent', 'treatment-open-dump', '%');

        // Mostrar datos de composición
        displayDataWithStatus(countryData, 'composition_food_organic', 'comp-food', '%');
        displayDataWithStatus(countryData, 'composition_paper_cardboard', 'comp-paper', '%');
        displayDataWithStatus(countryData, 'composition_plastic', 'comp-plastic', '%');
        displayDataWithStatus(countryData, 'composition_glass', 'comp-glass', '%');
        displayDataWithStatus(countryData, 'composition_metal', 'comp-metal', '%');
        displayDataWithStatus(countryData, 'composition_wood', 'comp-wood', '%');
        displayDataWithStatus(countryData, 'composition_rubber_leather', 'comp-rubber', '%');
        displayDataWithStatus(countryData, 'composition_yard_garden_green', 'comp-yard', '%');
        displayDataWithStatus(countryData, 'composition_other', 'comp-other', '%');

        // Mostrar datos de residuos especiales
        displayDataWithStatus(countryData, 'e_waste_tons_year', 'special-ewaste', ' Ton/año');
        displayDataWithStatus(countryData, 'hazardous_waste_tons_year', 'special-hazardous', ' Ton/año');

        // Mostrar resumen del cálculo de puntaje
        if (scoreSummaryElement && countryData.score_details_json) {
            try {
                const scoreDetails = JSON.parse(countryData.score_details_json);
                scoreSummaryElement.textContent = scoreDetails?.calculation_summary || "Resumen no disponible.";
            } catch (e) {
                console.error("Error al parsear score_details_json:", e);
                scoreSummaryElement.textContent = "Error al cargar el resumen del cálculo.";
            }
        }

        // Mostrar perfil analítico IA
        if (countryData.ai_profile_data) {
            displayAiProfileData(countryData.ai_profile_data);
        }

        // Crear gráficos
        createTreatmentChart(countryData);
        createPlotlyCompositionChart(countryData);

        // Mostrar mediciones adicionales
        if (countryData.measurements_data) {
            displayMeasurementsData(countryData.measurements_data);
        }

        // Mostrar lista de ciudades
        const citiesSectionDiv = document.getElementById('cities-list-section');
        if (indexData && Array.isArray(indexData.countries)) {
            const countryInfoFromIndex = indexData.countries.find(c => c.iso3 === iso3);
            if (countryInfoFromIndex && countryInfoFromIndex.cities && countryInfoFromIndex.cities.length > 0) {
                citiesSectionDiv.style.display = 'block';
                noCitiesElement.classList.add('hidden');
                cityListElement.innerHTML = '';
                
                countryInfoFromIndex.cities
                    .sort((a, b) => (a.name || "").localeCompare(b.name || ""))
                    .forEach(cityObject => {
                        const listItem = document.createElement('li');
                        const link = document.createElement('a');
                        link.href = `city_view.html?id=${cityObject.id}&iso3=${iso3}`;
                        link.textContent = cityObject.name;
                        link.className = 'text-sky-600 hover:text-sky-800 hover:underline';
                        listItem.appendChild(link);
                        cityListElement.appendChild(listItem);
                    });
            } else {
                citiesSectionDiv.style.display = 'block';
                noCitiesElement.classList.remove('hidden');
                cityListElement.innerHTML = '';
            }
        }
    })
    .catch(error => {
        loadingElement.style.display = 'none';
        console.error(`Error al cargar datos:`, error);
        errorElement.textContent = `Error al cargar los datos: ${error.message}`;
        errorElement.classList.remove('hidden');
        detailsElement.classList.add('hidden');
    });
});