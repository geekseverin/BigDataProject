/**
 * =====================================================
 * BIG DATA ANALYTICS - JAVASCRIPT APPLICATION
 * =====================================================
 * Fonctionnalités JavaScript pour l'interface Big Data
 */

// Configuration globale
const CONFIG = {
    API_BASE_URL: '/api',
    REFRESH_INTERVAL: 30000, // 30 secondes
    CHART_COLORS: {
        primary: 'rgba(54, 162, 235, 0.8)',
        secondary: 'rgba(255, 99, 132, 0.8)',
        success: 'rgba(75, 192, 192, 0.8)',
        warning: 'rgba(255, 206, 86, 0.8)',
        info: 'rgba(153, 102, 255, 0.8)',
        danger: 'rgba(255, 99, 132, 0.8)'
    }
};

// Gestionnaire d'état global
class AppState {
    constructor() {
        this.data = {
            sales: [],
            analytics: {},
            systemStatus: {},
            lastUpdate: null
        };
        this.charts = {};
        this.intervals = {};
    }
    
    updateData(key, value) {
        this.data[key] = value;
        this.data.lastUpdate = new Date();
        this.notifyComponents(key);
    }
    
    notifyComponents(key) {
        // Émettre un événement personnalisé
        const event = new CustomEvent('dataUpdated', {
            detail: { key, data: this.data[key] }
        });
        document.dispatchEvent(event);
    }
}

// Instance globale
const appState = new AppState();

// =====================================================
// UTILITAIRES
// =====================================================

class Utils {
    static formatCurrency(amount) {
        return new Intl.NumberFormat('fr-FR', {
            style: 'currency',
            currency: 'EUR'
        }).format(amount);
    }
    
    static formatNumber(number) {
        return new Intl.NumberFormat('fr-FR').format(number);
    }
    
    static formatDate(date) {
        return new Intl.DateTimeFormat('fr-FR', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        }).format(new Date(date));
    }
    
    static debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
    
    static showNotification(message, type = 'info') {
        const notification = document.createElement('div');
        notification.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
        notification.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
        notification.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        
        document.body.appendChild(notification);
        
        setTimeout(() => {
            if (notification.parentNode) {
                notification.remove();
            }
        }, 5000);
    }
    
    static async fetchData(endpoint, options = {}) {
        try {
            const response = await fetch(`${CONFIG.API_BASE_URL}${endpoint}`, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                ...options
            });
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            return await response.json();
        } catch (error) {
            console.error(`Erreur API ${endpoint}:`, error);
            Utils.showNotification(`Erreur de chargement: ${error.message}`, 'danger');
            throw error;
        }
    }
}

// =====================================================
// GESTIONNAIRE DE GRAPHIQUES
// =====================================================

class ChartManager {
    constructor() {
        this.charts = {};
    }
    
    createChart(canvasId, config) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) {
            console.warn(`Canvas ${canvasId} not found`);
            return null;
        }
        
        // Détruire le graphique existant s'il y en a un
        if (this.charts[canvasId]) {
            this.charts[canvasId].destroy();
        }
        
        const ctx = canvas.getContext('2d');
        this.charts[canvasId] = new Chart(ctx, config);
        
        return this.charts[canvasId];
    }
    
    updateChart(canvasId, data) {
        const chart = this.charts[canvasId];
        if (!chart) return;
        
        chart.data = data;
        chart.update();
    }
    
    destroyChart(canvasId) {
        if (this.charts[canvasId]) {
            this.charts[canvasId].destroy();
            delete this.charts[canvasId];
        }
    }
    
    destroyAll() {
        Object.keys(this.charts).forEach(id => {
            this.destroyChart(id);
        });
    }
}

const chartManager = new ChartManager();

// =====================================================
// GESTIONNAIRE D'API
// =====================================================

class APIManager {
    static async getSalesData() {
        return Utils.fetchData('/sales/by-region');
    }
    
    static async getCategoryData() {
        return Utils.fetchData('/sales/by-category');
    }
    
    static async getSystemStatus() {
        return Utils.fetchData('/system/status');
    }
    
    static async getPigResults() {
        return Utils.fetchData('/analytics/pig-results');
    }
    
    static async getSparkResults() {
        return Utils.fetchData('/analytics/spark-results');
    }
    
    static async getMongoCollections() {
        return Utils.fetchData('/mongodb/collections');
    }
}

// =====================================================
// GESTIONNAIRE DE DONNÉES EN TEMPS RÉEL
// =====================================================

class RealTimeManager {
    constructor() {
        this.intervals = {};
    }
    
    startPolling(key, fetchFunction, interval = CONFIG.REFRESH_INTERVAL) {
        this.stopPolling(key);
        
        const poll = async () => {
            try {
                const data = await fetchFunction();
                appState.updateData(key, data);
            } catch (error) {
                console.error(`Erreur polling ${key}:`, error);
            }
        };
        
        // Première exécution immédiate
        poll();
        
        // Puis répéter à intervalles
        this.intervals[key] = setInterval(poll, interval);
    }
    
    stopPolling(key) {
        if (this.intervals[key]) {
            clearInterval(this.intervals[key]);
            delete this.intervals[key];
        }
    }
    
    stopAllPolling() {
        Object.keys(this.intervals).forEach(key => {
            this.stopPolling(key);
        });
    }
}

const realTimeManager = new RealTimeManager();

// =====================================================
// GESTIONNAIRE D'INTERFACE
// =====================================================

class UIManager {
    static showLoading(element) {
        if (typeof element === 'string') {
            element = document.getElementById(element);
        }
        if (element) {
            element.classList.add('loading');
        }
    }
    
    static hideLoading(element) {
        if (typeof element === 'string') {
            element = document.getElementById(element);
        }
        if (element) {
            element.classList.remove('loading');
        }
    }
    
    static updateSystemStatus(status) {
        const statusElement = document.getElementById('system-status');
        if (!statusElement) return;
        
        let connectedServices = 0;
        let totalServices = 0;
        
        Object.keys(status).forEach(service => {
            if (service !== 'error' && status[service].status) {
                totalServices++;
                if (status[service].status === 'connected') {
                    connectedServices++;
                }
            }
        });
        
        const healthPercentage = Math.round((connectedServices / totalServices) * 100);
        
        if (healthPercentage === 100) {
            statusElement.className = 'badge bg-success';
            statusElement.innerHTML = '<i class="fas fa-check-circle me-1"></i>Tous services OK';
        } else if (healthPercentage > 50) {
            statusElement.className = 'badge bg-warning';
            statusElement.innerHTML = `<i class="fas fa-exclamation-triangle me-1"></i>${connectedServices}/${totalServices} services`;
        } else {
            statusElement.className = 'badge bg-danger';
            statusElement.innerHTML = '<i class="fas fa-times-circle me-1"></i>Services hors ligne';
        }
    }
    
    static animateCounter(elementId, targetValue, duration = 2000) {
        const element = document.getElementById(elementId);
        if (!element) return;
        
        const startValue = parseInt(element.textContent) || 0;
        const increment = (targetValue - startValue) / (duration / 16);
        let currentValue = startValue;
        
        const timer = setInterval(() => {
            currentValue += increment;
            if (currentValue >= targetValue) {
                currentValue = targetValue;
                clearInterval(timer);
            }
            element.textContent = Math.floor(currentValue);
        }, 16);
    }
}

// =====================================================
// INITIALISATION ET ÉVÉNEMENTS
// =====================================================

document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Big Data Analytics App initialized');
    
    // Démarrer le polling en temps réel
    realTimeManager.startPolling('systemStatus', APIManager.getSystemStatus, 30000);
    realTimeManager.startPolling('salesData', APIManager.getSalesData, 60000);
    
    // Écouter les mises à jour de données
    document.addEventListener('dataUpdated', function(event) {
        const { key, data } = event.detail;
        
        switch (key) {
            case 'systemStatus':
                UIManager.updateSystemStatus(data);
                break;
            case 'salesData':
                console.log('Sales data updated:', data);
                break;
        }
    });
    
    // Gestionnaire de visibilité de la page (économie de ressources)
    document.addEventListener('visibilitychange', function() {
        if (document.hidden) {
            realTimeManager.stopAllPolling();
            console.log('⏸️ Polling stopped (page hidden)');
        } else {
            realTimeManager.startPolling('systemStatus', APIManager.getSystemStatus, 30000);
            console.log('▶️ Polling resumed (page visible)');
        }
    });
    
    // Nettoyage avant fermeture de la page
    window.addEventListener('beforeunload', function() {
        realTimeManager.stopAllPolling();
        chartManager.destroyAll();
    });
    
    // Gestionnaires d'erreur globaux
    window.addEventListener('error', function(event) {
        console.error('Erreur globale:', event.error);
        Utils.showNotification('Une erreur inattendue est survenue', 'danger');
    });
    
    window.addEventListener('unhandledrejection', function(event) {
        console.error('Promise rejetée:', event.reason);
        Utils.showNotification('Erreur de traitement des données', 'warning');
        event.preventDefault();
    });
});

// =====================================================
// FONCTIONS GLOBALES EXPOSÉES
// =====================================================

// Pour utilisation dans les templates
window.BigDataApp = {
    Utils,
    ChartManager: chartManager,
    APIManager,
    UIManager,
    RealTimeManager: realTimeManager,
    appState
};

// Fonctions de convenance
window.refreshData = function() {
    Utils.showNotification('Actualisation des données...', 'info');
    realTimeManager.startPolling('systemStatus', APIManager.getSystemStatus, 1000);
    setTimeout(() => {
        Utils.showNotification('Données actualisées', 'success');
    }, 2000);
};

window.exportData = function(format = 'csv') {
    Utils.showNotification(`Export ${format.toUpperCase()} en cours...`, 'info');
    // Implémentation de l'export
};

console.log('✅ Big Data Analytics JavaScript loaded successfully');