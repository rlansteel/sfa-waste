/**
 * Funciones para crear y gestionar los gráficos
 */

/**
 * Crea un gráfico Plotly
 * @param {string} containerId - ID del contenedor del gráfico
 * @param {Array} data - Datos para el gráfico
 * @param {Object} layout - Layout para el gráfico
 * @param {string} loadingElementId - ID del elemento de carga
 * @param {string} noDataElementId - ID del elemento a mostrar cuando no hay datos
 */
function createPlotlyChart(containerId, data, layout, loadingElementId, noDataElementId) {
    const chartContainer = document.getElementById(containerId);
    const loadingEl = document.getElementById(loadingElementId);
    const noDataEl = document.getElementById(noDataElementId);

    if (loadingEl) {
        loadingEl.style.opacity = '0';
        setTimeout(() => {
            loadingEl.style.display = 'none';
        }, 300);
    }
    
    let hasPlottableData = false;
    
    if (Array.isArray(data) && data.length > 0) {
        hasPlottableData = data.some(trace => 
            (trace.x && trace.x.length > 0) || 
            (trace.y && trace.y.length > 0) ||
            (trace.values && trace.values.length > 0) 
        );
    }

    if (!hasPlottableData) {
        debug(`No hay datos suficientes para generar el gráfico ${containerId}`);
        if (noDataEl) {
            noDataEl.style.display = 'block';
            noDataEl.innerHTML = `
                <div class="flex flex-col items-center justify-center p-6">
                    <svg class="h-8 w-8 text-slate-400 mb-3" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                    <p class="text-slate-500">No hay datos suficientes para generar este gráfico.</p>
                </div>`;
        }
        if (chartContainer) chartContainer.style.display = 'none';
        return;
    }

    if (noDataEl) noDataEl.style.display = 'none';
    if (chartContainer) chartContainer.style.display = 'block';
    
    // Aplicar configuración común a todos los gráficos
    const commonConfig = {
        responsive: true,
        displaylogo: false,
        modeBarButtonsToRemove: ['select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d'],
        toImageButtonOptions: {
            format: 'png',
            filename: `grafico-${containerId}`,
            height: 500,
            width: 700,
            scale: 2
        }
    };
    
    try {
        // Aplicar estilos comunes al layout
        const enhancedLayout = {
            ...layout,
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            font: {
                family: 'Inter, sans-serif',
                size: 12,
                color: '#475569'
            },
            margin: layout.margin || { l: 50, r: 20, b: 70, t: 20 },
            hovermode: 'closest',
            hoverlabel: {
                bgcolor: 'white',
                bordercolor: '#e2e8f0',
                font: {
                    family: 'Inter, sans-serif',
                    size: 12,
                    color: '#0f172a'
                },
                align: 'left'
            }
        };
        
        // Animar la creación del gráfico con un efecto de fade in
        Plotly.newPlot(containerId, data, enhancedLayout, commonConfig);
        
        // Añadir evento de hover para mostrar detalles adicionales
        const chartElement = document.getElementById(containerId);
        chartElement.on('plotly_hover', function(data) {
            // Si queremos añadir interactividad adicional durante el hover
        });
        
        debug(`Gráfico ${containerId} creado con éxito`);
    } catch (error) {
        console.error(`Error al crear gráfico ${containerId}:`, error);
        if (noDataEl) {
            noDataEl.style.display = 'block';
            noDataEl.innerHTML = `
                <div class="flex flex-col items-center justify-center p-6">
                    <svg class="h-8 w-8 text-red-500 mb-3" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                    </svg>
                    <p class="text-slate-700">Error al generar el gráfico. Consulta la consola para más detalles.</p>
                </div>`;
        }
    }
}

/**
 * Crea el gráfico boxplot de residuos per cápita por grupo de ingresos
 * @param {Array} countriesData - Datos de países
 */
function createWastePerCapitaBoxplot(countriesData) {
    debug("Generando boxplot de residuos per cápita...");
    
    const relevantData = countriesData.filter(c =>
        c.income && c.waste_per_capita_kg_day !== null && !isNaN(parseFloat(c.waste_per_capita_kg_day))
    );
    
    debug(`Datos para boxplot: ${relevantData.length} países con datos válidos`);
    
    // Agrupar por tipo de ingresos
    const dataByIncome = {};
    const countryNamesByIncome = {}; // Nuevo objeto para almacenar nombres de países
    CONFIG.INCOME_ORDER.forEach(incomeGroup => { 
        dataByIncome[incomeGroup] = [];
        countryNamesByIncome[incomeGroup] = []; // Inicializar array de nombres
    });
    
    relevantData.forEach(country => {
        let incomeKey = country.income;
        
        // Mapear nombres largos a códigos si es necesario
        if (incomeKey === 'High income') incomeKey = 'HIC';
        else if (incomeKey === 'Upper middle income') incomeKey = 'UMC';
        else if (incomeKey === 'Lower middle income') incomeKey = 'LMC';
        else if (incomeKey === 'Low income') incomeKey = 'LIC';
        
        if (dataByIncome[incomeKey]) {
            dataByIncome[incomeKey].push(parseFloat(country.waste_per_capita_kg_day));
            countryNamesByIncome[incomeKey].push(country.country_name); // Guardar nombre del país
        }
    });
    
    // Mejorar el mapeo de códigos a nombres completos
    const incomeLabels = {
        'HIC': 'Alto (HIC)',
        'UMC': 'Medio-Alto (UMC)',
        'LMC': 'Medio-Bajo (LMC)',
        'LIC': 'Bajo (LIC)'
    };
    
    // Crear trazas para cada grupo de ingresos
    const traces = CONFIG.INCOME_ORDER.map(incomeGroup => {
        const incomeColor = CONFIG.INCOME_GROUP_COLORS[incomeGroup] || CONFIG.INCOME_GROUP_COLORS['Default'];
        
        return {
            y: dataByIncome[incomeGroup], 
            type: 'box', 
            name: incomeLabels[incomeGroup] || incomeGroup,
            boxpoints: 'all', 
            jitter: 0.4, 
            pointpos: -1.8,
            text: countryNamesByIncome[incomeGroup], // Añadir nombres como texto
            hovertemplate: '<b>%{text}</b><br>%{y:.2f} kg/día<extra></extra>', // Formato del tooltip
            marker: { 
                color: incomeColor,
                opacity: 0.7,
                size: 6, // Tamaño reducido para mejor visualización
                line: {
                    width: 1,
                    color: 'rgba(0,0,0,0.3)'
                }
            },
            line: {
                color: incomeColor
            },
            fillcolor: incomeColor,
            boxmean: true // Mostrar media
        };
    }).filter(trace => trace.y && trace.y.length > 0);
    
    // Definir layout
    const layout = {
        title: { text: null }, 
        yaxis: { 
            title: { 
                text: 'kg / persona / día',
                font: { size: 13, color: '#475569' }
            }, 
            zeroline: false, 
            gridcolor: '#e2e8f0',
            automargin: true
        },
        xaxis: { 
            title: { 
                text: 'Nivel de Ingresos',
                font: { size: 13, color: '#475569' }
            }, 
            categoryorder: 'array', 
            categoryarray: CONFIG.INCOME_ORDER.map(code => incomeLabels[code] || code),
            automargin: true
        },
        showlegend: false,
        annotations: [{
            xref: 'paper',
            yref: 'paper',
            x: 0.5,
            y: -0.15,
            text: 'Los puntos muestran los valores individuales por país. La línea central es la mediana.',
            showarrow: false,
            font: {
                size: 10,
                color: '#64748B'
            }
        }]
    };
    
    createPlotlyChart('boxplot-container', traces, layout, 'loading-boxplot', 'no-boxplot-data');
}

/**
 * Crea el gráfico de dispersión de residuos per cápita vs PIB per cápita
 * @param {Array} countriesData - Datos de países
 */
function createWpcVsGdpScatterPlot(countriesData) {
    debug("Generando scatter plot de residuos vs GDP...");
    
    // Normalizar grupo de ingresos
    function normalizeIncomeGroup(income) {
        switch(income) {
            case 'High income': return 'HIC';
            case 'Upper middle income': return 'UMC';
            case 'Lower middle income': return 'LMC';
            case 'Low income': return 'LIC';
            default: return income;
        }
    }

    // Filtrar países con datos completos y válidos
    const plotData = countriesData.filter(country => {
        const gdp = parseFloat(country.gdp_per_capita_usd);
        const waste = parseFloat(country.waste_per_capita_kg_day);
        const hasIncome = !!country.income;

        const isValid = !isNaN(gdp) && gdp > 0 && !isNaN(waste) && waste > 0 && hasIncome;

        if (!isValid && CONFIG.DEBUG) {
            const missing = [];
            if (isNaN(gdp) || gdp <= 0) missing.push('GDP');
            if (isNaN(waste) || waste <= 0) missing.push('Waste');
            if (!hasIncome) missing.push('Income group');
            debug(`País ${country.country_name} (${country.iso3}) excluido: datos faltantes [${missing.join(', ')}]`);
        }

        return isValid;
    });

    debug(`Datos válidos para scatter plot: ${plotData.length} países`);

    if (plotData.length === 0) {
        const noDataEl = document.getElementById('no-scatter-gdp-data');
        const containerEl = document.getElementById('scatter-gdp-container');
        if (noDataEl) noDataEl.style.display = 'block';
        if (containerEl) containerEl.style.display = 'none';
        return;
    }

    // Mejorar las etiquetas de grupos de ingresos
    const incomeLabels = {
        'HIC': 'Alto (HIC)',
        'UMC': 'Medio-Alto (UMC)',
        'LMC': 'Medio-Bajo (LMC)',
        'LIC': 'Bajo (LIC)'
    };

    // Agrupar por nivel de ingresos normalizado
    const groupedData = {};
    plotData.forEach(country => {
        const group = normalizeIncomeGroup(country.income);
        if (!groupedData[group]) {
            groupedData[group] = [];
        }
        groupedData[group].push(country);
    });

    // Crear trazas para cada grupo
    const traces = Object.entries(groupedData).map(([group, countries]) => {
        const sizes = countries.map(c => {
            // Escalar tamaño de burbujas según población
            const pop = parseFloat(c.population);
            if (!isNaN(pop) && pop > 0) {
                return Math.max(8, Math.min(25, Math.sqrt(pop) / 5000));
            }
            return 8; // Tamaño por defecto
        });
        
        return {
            x: countries.map(c => parseFloat(c.gdp_per_capita_usd)),
            y: countries.map(c => parseFloat(c.waste_per_capita_kg_day)),
            text: countries.map(c => c.country_name),
            customdata: countries.map(c => ({
                iso3: c.iso3,
                population: c.population ? parseInt(c.population).toLocaleString('es-ES') : 'N/A'
            })),
            mode: 'markers',
            type: 'scatter',
            name: incomeLabels[group] || group,
            marker: {
                size: sizes,
                color: CONFIG.INCOME_GROUP_COLORS[group] || CONFIG.INCOME_GROUP_COLORS['Default'],
                opacity: 0.8,
                line: { 
                    width: 1, 
                    color: 'rgba(0,0,0,0.3)' 
                }
            },
            hovertemplate: '<b>%{text} (%{customdata.iso3})</b><br>' +
                           'PIB: %{x:.0f} USD per cápita<br>' +
                           'Residuos: %{y:.2f} kg/día<br>' +
                           'Población: %{customdata.population}<extra></extra>'
        };
    });

    const layout = {
        title: { text: null },
        xaxis: {
            title: { 
                text: 'PIB Per Cápita (USD, Escala Log)',
                font: { size: 13, color: '#475569' }
            },
            type: 'log',
            automargin: true,
            showgrid: true,
            gridcolor: '#e2e8f0'
        },
        yaxis: {
            title: { 
                text: 'Residuos Per Cápita (kg/día)',
                font: { size: 13, color: '#475569' }
            },
            automargin: true,
            showgrid: true,
            gridcolor: '#e2e8f0'
        },
        hovermode: 'closest',
        legend: {
            title: { 
                text: 'Nivel de Ingresos',
                font: { size: 12, color: '#475569' }
            },
            orientation: 'h',
            yanchor: 'bottom',
            y: -0.2,
            xanchor: 'center',
            x: 0.5,
            font: { size: 11 }
        },
        annotations: [{
            xref: 'paper',
            yref: 'paper',
            x: 0.5,
            y: -0.32,
            text: 'El tamaño de las burbujas representa la población total del país.',
            showarrow: false,
            font: {
                size: 10,
                color: '#64748B'
            }
        }]
    };

    createPlotlyChart('scatter-gdp-container', traces, layout, 'loading-scatter-gdp', 'no-scatter-gdp-data');
}

/**
 * Crea el histograma de puntajes de calidad
 * @param {Array} countriesData - Datos de países
 */
function createQualityScoreHistogram(countriesData) {
    debug("Generando histograma de puntajes de calidad...");
    
    const qualityScores = countriesData
        .map(c => parseFloat(c.data_quality_score))
        .filter(score => !isNaN(score));
    
    debug(`Datos para histograma: ${qualityScores.length} puntajes de calidad válidos`);
    
    // Calculamos cortes para colores personalizados
    const binSize = 10;
    const colorScale = [
        [0, '#ef4444'],      // Rojo (0-20)
        [0.2, '#f97316'],    // Naranja (20-40)
        [0.4, '#f59e0b'],    // Ámbar (40-60)
        [0.6, '#84cc16'],    // Lima (60-80)
        [0.8, '#10b981'],    // Esmeralda (80-100)
        [1, '#10b981']       // Esmeralda (final)
    ];
    
    const trace = {
        x: qualityScores, 
        type: 'histogram',
        opacity: 0.8,
        marker: { 
            color: qualityScores,
            colorscale: colorScale,
            line: {
                width: 1,
                color: 'rgba(0,0,0,0.1)'
            }
        }, 
        autobinx: false,
        xbins: { 
            start: 0, 
            end: 100, 
            size: binSize 
        }
    };
    
    const layout = {
        title: { text: null },
        xaxis: { 
            title: { 
                text: 'Puntaje de Calidad (0-100)',
                font: { size: 13, color: '#475569' }
            },
            range: [0, 100],
            tickvals: [0, 20, 40, 60, 80, 100],
            showgrid: true,
            gridcolor: '#e2e8f0',
            automargin: true
        },
        yaxis: { 
            title: { 
                text: 'Número de Países',
                font: { size: 13, color: '#475569' }
            },
            showgrid: true,
            gridcolor: '#e2e8f0',
            automargin: true
        },
        bargap: 0.05,
        annotations: [
            {
                x: 10,
                y: 0,
                xref: 'x',
                yref: 'paper',
                text: 'Baja',
                showarrow: false,
                font: { color: '#ef4444', size: 11 },
                yanchor: 'bottom',
                yshift: 10
            },
            {
                x: 50,
                y: 0,
                xref: 'x',
                yref: 'paper',
                text: 'Media',
                showarrow: false,
                font: { color: '#f59e0b', size: 11 },
                yanchor: 'bottom',
                yshift: 10
            },
            {
                x: 85,
                y: 0,
                xref: 'x',
                yref: 'paper',
                text: 'Alta',
                showarrow: false,
                font: { color: '#10b981', size: 11 },
                yanchor: 'bottom',
                yshift: 10
            }
        ]
    };
    
    createPlotlyChart('quality-hist-container', [trace], layout, 'loading-quality-hist', 'no-quality-hist-data');
}

/**
 * Actualiza todos los gráficos con los datos filtrados
 * @param {Array} filteredCountries - Países filtrados
 */
function updateAllCharts(filteredCountries) {
    if (!filteredCountries || !Array.isArray(filteredCountries)) {
        debug("No hay datos válidos para actualizar los gráficos");
        return;
    }
    
    debug(`Actualizando gráficos con ${filteredCountries.length} países`);
    
    createWastePerCapitaBoxplot(filteredCountries);
    createWpcVsGdpScatterPlot(filteredCountries);
    createQualityScoreHistogram(filteredCountries);
}