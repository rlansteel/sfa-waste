/**
 * Configuración global para la aplicación de visualización de datos
 */

// Rutas de datos
const UI_DATA_DIR = './output/ui_data/';
const GEOJSON_ISO_PROPERTY = 'ISO3166-1-Alpha-3';

const CONFIG = {
    REGION_COLORS: {
        // Colores basados en los códigos de TUS DATOS
        // ¡DEBES VERIFICAR Y AJUSTAR ESTOS CÓDIGOS Y COLORES!
        'EAS': '#60a5fa', // Ejemplo si este es un código directo
        'ECS': '#3b82f6',                     // Ejemplo para Europa y Asia Central
        'LCN': '#34d399',                     // Ejemplo para América Latina y el Caribe
        'MEA': '#10b981',                     // Ejemplo para Medio Oriente y Norte de África
        'NAC': '#f472b6',                     // Ejemplo para América del Norte
        'SAS': '#ec4899',                     // Ejemplo para Asia del Sur
        'SSF': '#fb923c',                     // Ejemplo para África Subsahariana

       

        'Default': '#cbd5e1'
    },

    REGION_DISPLAY_NAMES: {
        // Mapeo de TUS CÓDIGOS a nombres legibles en español
        // ¡DEBES VERIFICAR Y AJUSTAR ESTOS MAPEOS!
        'EAS': 'Asia Oriental y del Sur', // Si es un código directo
        'ECS': 'Europa y Asia Central',
        'LCN': 'América Latina y el Caribe',
        'MEA': 'Medio Oriente y Norte de África',
        'NAC': 'América del Norte',
        'SAS': 'Asia del Sur',
        'SSF': 'África Subsahariana',

        // Mapeos de nombres completos estándar (por si acaso y para consistencia)
        'East Asia & Pacific': 'Asia Oriental y Pacífico',
        'Europe & Central Asia': 'Europa y Asia Central',
        'Latin America & Caribbean': 'América Latina y el Caribe',
        'Middle East & North Africa': 'Medio Oriente y Norte de África',
        'North America': 'América del Norte',
        'South Asia': 'Asia del Sur',
        'Sub-Saharan Africa': 'África Subsahariana',

        // Fallbacks
        'Default': 'Región Desconocida',
        'Sin Región Especificada': 'Sin Región Especificada'
    },

    INCOME_GROUP_COLORS: {
        'High income': '#0ea5e9',
        'Upper middle income': '#6366f1',
        'Lower middle income': '#8b5cf6',
        'Low income': '#d946ef',
        'HIC': '#0ea5e9',
        'UMC': '#6366f1',
        'LMC': '#8b5cf6',
        'LIC': '#d946ef',
        'Default': '#94a3b8'
    },

    QUALITY_COLORS: {
        high: '#10b981',
        medium: '#f59e0b',
        low: '#ef4444',
        default: '#64748b'
    },

    INCOME_ORDER: ['LIC', 'LMC', 'UMC', 'HIC'],

    DEBUG: false
};

// Funciones de depuración (toggleDebug, debug) permanecen igual...
function toggleDebug() {
    CONFIG.DEBUG = !CONFIG.DEBUG;
    const debugPanel = document.getElementById('debug-info');
    if (debugPanel) {
        debugPanel.style.display = CONFIG.DEBUG ? 'block' : 'none';
        if (CONFIG.DEBUG) {
            debugPanel.innerHTML = '<div class="font-semibold text-green-400 mb-2">Modo de depuración ACTIVADO</div>';
        }
    }
    console.log(`Modo de depuración: ${CONFIG.DEBUG ? 'ACTIVADO' : 'DESACTIVADO'}`);
}

function debug(message) {
    if (!CONFIG.DEBUG) return;
    console.log("[DEBUG]", message);
    const debugPanel = document.getElementById('debug-info');
    if (debugPanel && debugPanel.style.display !== 'none') {
        const timestamp = new Date().toLocaleTimeString();
        const newLogEntry = document.createElement('div');
        newLogEntry.textContent = `[${timestamp}] ${message}`;
        if (debugPanel.childNodes.length > 1) {
            debugPanel.insertBefore(newLogEntry, debugPanel.childNodes[1]);
        } else {
            debugPanel.appendChild(newLogEntry);
        }
        const maxLogs = 100;
        while (debugPanel.childNodes.length > maxLogs + 1) {
            debugPanel.removeChild(debugPanel.lastChild);
        }
    }
}

document.addEventListener('keydown', function(e) {
    if (e.ctrlKey && e.shiftKey && (e.key === 'D' || e.code === 'KeyD')) {
        e.preventDefault();
        toggleDebug();
    }
});
