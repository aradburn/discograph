import { loading } from './loading';
import { initRelations } from './relations';
import { initNetwork } from './network/init';
import { restartForceLayout, stopForceLayout } from './network/forceLayout';
import { initRoles } from './roles';
import { initSvg, printSvg } from './svg';
import { initTypeahead } from './typeahead';
import { dg } from './dg';
import { DiscographFsm } from './fsm';
import { Tooltip } from 'bootstrap';

// Constants
export const VIEWPORT_SIZE_MULTIPLIER = 3.0;
export const SVG_SCALING_MULTIPLIER = 0.8;

// Utility functions
export const clamp = (num, lower, upper) => Math.min(Math.max(num, lower), upper);

export const debounce = (func, wait) => {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
};

/**
 * Display a message to the user using Bootstrap alerts
 * @param {string} type - The type of alert ('success', 'danger', 'warning', etc.)
 * @param {string} message - The message to display
 */
export const showMessage = (type, message) => {
    const text = `
        <div class="alert alert-${type} alert-dismissible" role="alert">
            <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                <span aria-hidden="true">&times;</span>
            </button>
            ${message}
        </div>
    `;
    const flashElement = document.querySelector('#flash');
    if (flashElement) {
        flashElement.insertAdjacentHTML('beforeend', text);
    }
};

/**
 * Clear all displayed messages after a specified delay
 * @param {number} delay - The delay in milliseconds before clearing messages
 */
export const clearMessages = (delay) => {
    setTimeout(() => {
        const flashElement = document.querySelector('#flash');
        if (flashElement) {
            flashElement.innerHTML = '';
        }
    }, delay);
};

/**
 * Initialize the application
 * Sets up event handlers for UI controls and initializes various components
 */
export const initApp = () => {
    // Initialize all required components
    initWindow();
    initSvg();
    initNetwork();
    initRelations();
    // @ts-ignore
    initRoles(dgRoles);
    initTypeahead();

    loading.init(dg.svg_dimensions);

    // Random request button handler
    const requestRandomButton = document.querySelector('#request-random');
    const handleRandomRequest = (event) => {
        event.preventDefault();
        if (requestRandomButton) {  
            requestRandomButton.dispatchEvent(new CustomEvent('discograph:request-random', { bubbles: true }));
        }
    };
    if (requestRandomButton) {
        requestRandomButton.addEventListener('click', handleRandomRequest);
        requestRandomButton.addEventListener('touchstart', handleRandomRequest);
    }

    // Layout control button handlers
    const startLayoutButton = document.querySelector('#start-layout');
    const handleStartLayout = (event) => {
        event.preventDefault();
        restartForceLayout();
    };
    if (startLayoutButton) {
        startLayoutButton.addEventListener('click', handleStartLayout);
        startLayoutButton.addEventListener('touchstart', handleStartLayout);
    }

    const stopLayoutButton = document.querySelector('#stop-layout');
    const handleStopLayout = (event) => {
        event.preventDefault();
        stopForceLayout();
    };
    if (stopLayoutButton) {
        stopLayoutButton.addEventListener('click', handleStopLayout);
        stopLayoutButton.addEventListener('touchstart', handleStopLayout);
    }

    // Print button handler
    const printButton = document.querySelector('#print');
    const handlePrint = (event) => {
        event.preventDefault();
        printSvg(dg.svg_dimensions[0], dg.svg_dimensions[1]);
    };
    if (printButton) {
        printButton.addEventListener('click', handlePrint);
        printButton.addEventListener('touchstart', handlePrint);
    }

    // Initialize tooltips using Bootstrap 5
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    const tooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => {
        return new Tooltip(tooltipTriggerEl, {
            trigger: 'hover'
        });
    });

    // Initialize the Discograph Finite State Machine
    dg.fsm = new DiscographFsm();
    console.log('discograph initialized.');
};

/**
 * Initialize window and SVG dimensions
 * Calculates and sets up various dimension-related properties based on the window size and device pixel ratio
 * Properties set:
 * - dg.dpr: Device pixel ratio
 * - dg.dimensions: SVG container dimensions
 * - dg.svg_dimensions: Actual SVG coordinate dimensions (scaled by viewport multiplier and DPR)
 * - dg.network.newNodeCoords: Starting coordinates for new nodes
 */
export function initWindow() {
    var w = window,
        d = document,
        e = d.documentElement,
        g = d.getElementsByTagName('body')[0];
    dg.dpr = w.devicePixelRatio;
    console.log("window devicePixelRatio: ", dg.dpr);

    const svgContainer = d.getElementById('svg-container-fluid');
    if (svgContainer) {
        dg.dimensions = [
            svgContainer.clientWidth,
            svgContainer.clientHeight,
        ];
        console.log("svg panel dimensions: ", dg.dimensions);

        dg.svg_dimensions = [
            dg.dimensions[0] * VIEWPORT_SIZE_MULTIPLIER * dg.dpr,
            dg.dimensions[1] * VIEWPORT_SIZE_MULTIPLIER * dg.dpr,
        ];
        console.log("svg coord dimensions: ", dg.svg_dimensions);

        // All nodes start at center of the screen
        dg.network.newNodeCoords = [
            dg.svg_dimensions[0] / 2,
            dg.svg_dimensions[1] / 2,
        ];
        console.log("svg newNodeCoords: ", dg.network.newNodeCoords);
    }
}