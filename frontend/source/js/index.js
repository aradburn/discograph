/**
 * @fileoverview Main entry point for the Discograph application.
 * This module initializes the application and sets up module exports.
 * @module discograph
 * @version 0.2
 */
import 'jquery';
import jQuery from "jquery";
// Inject jQuery into the global scope
Object.assign(window, { $: jQuery, jQuery });

// Import our custom CSS
import '../css/discograph.scss'

// Import all of Bootstrap's JS
import * as bootstrap from 'bootstrap'

import { initApp } from './init'

// Initialize the application when the DOM is loaded
document.addEventListener('DOMContentLoaded', initApp);
