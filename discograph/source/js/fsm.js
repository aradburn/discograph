/**
 * DiscographFsm - A Finite State Machine implementation for the Discograph application
 * Built on machina.js FSM framework to manage application state and transitions
 * Handles network visualization, radial view transitions, and entity selection
 * @typedef {Object} MachinaFsm
 * @property {Function} extend - Method to extend the FSM with new properties
 */

import { loading } from './loading';
import { clearRelationsLayer, createRadialChart, setRelationsData } from './relations';
import { getSelectedRoles } from './roles';
import { resetSvgSize, setSvgSize } from './svg';
import { startForceLayout, restartForceLayout, stopForceLayout, processJson } from './network/forceLayout';
import { resetNetworkTransform } from './network/init';
import { debounce } from './init';
import { dg } from './dg';
import { initWindow } from './init';
import { ALPHA } from './network/forceLayout';
import { RequestNetworkEvent, SelectEntityEvent } from './network/events';
import * as d3 from 'd3';

// @ts-ignore - Ignore TypeScript error for machina.Fsm property
export const DiscographFsm = machina.Fsm.extend({
    /**
     * Initializes the FSM with event listeners and initial state setup
     * Sets up window event handlers for network requests, entity selection,
     * view transitions, and browser history management
     * @param {Object} options - Initialization options
     */
    initialize: function(options) {
        const self = this;
        
        // Handle network data request events
        window.addEventListener('discograph:request-network', function(event) {
            if (event instanceof RequestNetworkEvent && event.detail) {
                const entityKey = event.detail.entityKey;
                const pushHistory = event.detail.pushHistory;
                self.requestNetwork(entityKey, pushHistory);
            }
        });
        
        // Handle random entity request events
        window.addEventListener('discograph:request-random', function(event) {
            self.requestRandom();
        });
        
        // Handle entity selection events
        window.addEventListener('discograph:select-entity', function(event) {
            if (event instanceof SelectEntityEvent && event.detail) {
                const entityKey = event.detail.entityKey;
                const fixed = event.detail.fixed;
                self.selectEntity(entityKey, fixed);
            }
        });

        // Handle view transition events
        window.addEventListener('discograph:show-network', function(event) {
            self.showNetwork();
        });
        window.addEventListener('discograph:show-radial', function(event) {
            self.showRadial();
        });
//        $(window).on('select2:selecting', function(event) {
//            self.rolesBackup = $('#filter select').val();
//        });
//        $(window).on('select2:unselecting', function(event) {
//            self.rolesBackup = $('#filter select').val();
//        });
        
        // Handle browser history navigation
        window.onpopstate = function(event) {
            console.log("FSM window.onpopstate: ", event);
            if (!event || !event.state || !event.state.key) {
                return;
            }
            const entityKey = event.state.key;
            const [entityType, entityId] = entityKey.split("-");
            const url = `/${entityType}/${entityId}`;
            const pushHistory = false;
            window.dispatchEvent(new RequestNetworkEvent(entityKey, pushHistory));
        };

        // Handle window resize events with debounce
        const handleResize = debounce(function(event) {
            console.log("FSM window.onresize: ", event);
            window.location.reload();

            // Reset and reinitialize SVG dimensions
            resetSvgSize();
            initWindow();
            setSvgSize();

            // Center the visualization
            const transform = `translate(${dg.dimensions[0] / 2},${dg.dimensions[1] / 2})`;
            document.querySelectorAll('.centered').forEach(el => {
                // @ts-ignore
                el.style.transition = 'transform 250ms';
                // @ts-ignore
                el.style.transform = transform;
            });

            // Restart force layout if in network view
            if (self.state == 'viewing-network') {
                console.log("start d3 layout");
//                dg_network_processJson(dg.network.data.json);
//                dg_network_startForceLayout();
                restartForceLayout(ALPHA / 10.0);
            }
        }, 50);

        window.addEventListener('resize', handleResize);

        // Handle SVG mousedown events for view transitions
        const svgDocument = document.getElementById('svg');
        if (svgDocument) {
            svgDocument.addEventListener('mousedown', function(event) {
                if (self.state == 'viewing-network') {
                    self.selectEntity(null);
                } else if (self.state == 'viewing-radial') {
                    self.showNetwork();
                }
            });
        }

        // Log all FSM events for debugging
        self.on("*", function(event, data) {
            console.log("FSM: ", event, data);
        });

        // Initialize application state
        this.loadInlineData();
        this.toggleRadial(false);
        self.rolesBackup = getSelectedRoles() || [];
    },
    namespace: 'discograph',
    initialState: 'uninitialized',

    /**
     * FSM States Configuration
     * Defines the behavior and transitions for each application state
     */
    states: {
        /**
         * Initial state before data loading
         * Handles initial network requests and data loading
         */
        'uninitialized': {
            'request-network': function(entityKey) {
                console.log("UNINITIALIZED request-network");
                this.requestNetwork(entityKey);
            },
            'request-random': function() {
                console.log("UNINITIALIZED request-random");
                this.requestRandom();
            },
            'load-inline-data': function(data) {
                console.log("UNINITIALIZED load-inline-data");
                var params = {'roles': getSelectedRoles() || []};
                this.deferAndTransition("requesting");
                this.handle("received-network", data, false, params);
                console.log("load-inline-data end");
            },
        },

        /**
         * Network visualization view state
         * Handles network visualization interactions and transitions
         */
        'viewing-network': {
            '_onEnter': function() {
                console.log("VIEWING-NETWORK _onEnter");
                this.toggleNetwork(true);
            },
            '_onExit': function() {
                console.log("VIEWING-NETWORK _onExit");
                this.toggleNetwork(false);
            },
            'request-network': function(entityKey) {
                console.log("VIEWING-NETWORK request-network");
                this.requestNetwork(entityKey);
            },
            'request-random': function() {
                console.log("VIEWING-NETWORK request-random");
                this.requestRandom();
            },
            'show-radial': function() {
                console.log("VIEWING-NETWORK show-radial");
                if (dg.network.pageData.selectedNodeKey) {
                    this.requestRadial(dg.network.pageData.selectedNodeKey);
                }
            },
            /**
             * Handles entity selection in network view
             * Updates visual state and entity details
             * @param {string} entityKey - Selected entity key
             * @param {boolean} fixed - Whether to fix entity position
             */
            'select-entity': function(entityKey, fixed) {
                console.log("VIEWING-NETWORK select-entity", entityKey, fixed);
                dg.network.pageData.selectedNodeKey = entityKey;
                if (entityKey !== null) {
                    // Select and highlight the target node
                    var nodeOn = dg.network.layers.root?.selectAll('#' + entityKey);
                    var nodeOff = dg.network.layers.root?.selectAll('.node:not(#' + entityKey + ')');
                    var linkKeys = nodeOn?.datum().links;
                    
                    // Filter and highlight connected links
                    var linkOn = dg.network.selections.link?.filter(function(d) {
                        return linkKeys.indexOf(d.key) >= 0;
                    });
                    var linkOff = dg.network.selections.link?.filter(function(d) {
                        return linkKeys.indexOf(d.key) == -1;
                    });

                    // Update entity details display
                    var node = dg.network.data.nodeMap.get(entityKey);
                    var url = 'http://discogs.com/' + node.type + '/' + node.id;
                    const entityName = document.getElementById('entity-name');
                    const entityLink = document.getElementById('entity-link');
                    if (entityName && entityLink) {
                        entityName.textContent = node.name;
                        // @ts-ignore
                        entityLink.href = url;
                    }
                    const entityDetails = document.getElementById('entity-details');
                    if (entityDetails) {
                        entityDetails.classList.remove('hidden');
                        entityDetails.style.display = 'block';
                    }
                    const navbarTitle = document.getElementById('navbar-title');
                    if (navbarTitle) {
                        navbarTitle.textContent = node.name;
                    }

                    // Update visual state
                    nodeOn?.raise();
                    nodeOn?.classed('selected', true);
                    if (fixed) {
                        //nodeOn.each(function(d) { d.fixed = true; });
                        node.fixed = true;
                    }
                    // linkOn.classed('selected', true);
                } else {
                    // Clear selection
                    var nodeOff = dg.network.layers.root?.selectAll('.node');
                    // @ts-ignore
                    var linkOff = dg.network.selections.link;
//  TODO                  $('#entity-details').disabled();
//                    $('#entity-details').hide();
                }

                // Reset unselected nodes and links
                if (nodeOff) {
                    nodeOff.classed('selected', false);
                    nodeOff.each(function(d) { d.fixed = false; });
                }
                if (linkOff) {
                    linkOff.classed('selected', false);
                }
            },
        },

        /**
         * Radial visualization view state
         * Handles radial view transitions and interactions
         */
        'viewing-radial': {
            '_onEnter': function() {
                console.log("VIEWING-RADIAL _onEnter");
                this.toggleRadial(true);
                clearRelationsLayer();
                createRadialChart();
            },
            '_onExit': function() {
                console.log("VIEWING-RADIAL _onExit");
                clearRelationsLayer();
                this.toggleRadial(false);
            },
            'request-network': function(entityKey) {
                console.log("VIEWING-RADIAL request-network");
                this.requestNetwork(entityKey);
            },
            'request-random': function() {
                console.log("VIEWING-RADIAL request-random");
                this.requestRandom();
            },
            'show-network': function() {
                console.log("VIEWING-RADIAL show-network");
                this.transition('viewing-network');
            },
        },

        /**
         * Data loading and transition state
         * Handles API requests and data processing
         */
        'requesting': {
            '_onEnter': function(fsm, entityKey, pushHistory) {
                console.log("REQUESTING _onEnter");
                this.toggleLoading(true);
                this.toggleFilter(false);
            },
            '_onExit': function() {
                console.log("REQUESTING _onExit");
                this.toggleLoading(false);
                this.toggleFilter(true);
            },
            'errored': function(error) {
                console.log("REQUESTING errored");
                this.handleError(error);
            },
            /**
             * Handles received network data
             * Processes and displays network visualization
             */
            'received-network': function(data, pushHistory, params) {
                console.log("REQUESTING received network", data);
                var newParams = {'roles': getSelectedRoles() || []};
                var entityKey = data.center.key;
                
                // Update application state
                dg.network.data.json = JSON.parse(JSON.stringify(data));
                document.title = 'Discograph2: ' + data.center.name;
                document.body.id = entityKey;
                
                // Update browser history if needed
                if (pushHistory === true) {
                    this.pushState(entityKey, newParams);
                }

                // Process and display network data
                console.log("received-network dg_network_processJson");
                processJson(data);
                console.log("received-network dg_network_selectPage");
                dg.network.pageData.nodes = Array.from(dg.network.data.nodeMap.values());
                dg.network.pageData.links = Array.from(dg.network.data.linkMap.values());
                resetNetworkTransform()
                
                // Start force layout and transition to network view
                console.log("received-network startForceLayout");
                startForceLayout();
                this.deferAndTransition('viewing-network');
                if (dg.network.data.json) {
                    this.selectEntity(dg.network.data.json.center.key, false);
                }
            },
            'received-random': function(data) {
                console.log("REQUESTING received-random data: ", data);
                this.requestNetwork(data.center, true);
            },
            /**
             * Handles received radial view data
             * Processes and displays radial visualization
             */
            'received-radial': function(data) {
                console.log("REQUESTING received-radial data: ", data);
                setRelationsData(data);

                console.log("REQUESTING received radial about to transition");
                this.transition('viewing-radial');
            },
        },
    },
    /**
     * Error handling for API requests and state transitions
     * Displays error message and reverts to previous state
     * @param {Object} error - Error object containing status and message
     */
    handleError: function(error) {
        var message = 'Something went wrong!';
        var status = error.status;
        if (status == 0) {
            status = 404;
        } else if (status == 429) {
            message = 'Hey, slow down, buddy. Give it a minute.'
        }
        var text = [
            '<div class="alert alert-danger alert-dismissible" role="alert">',
            '<button type="button" class="close" data-dismiss="alert" aria-label="Close">',
            '<span aria-hidden="true">&times;</span>',
            '</button>',
            '<strong>' + status + '!</strong> ' + message,
            '</div>'
            ].join('');
        
        const flash = document.getElementById('flash');
        if (flash) {
            flash.innerHTML += text;
        }

        if (this.rolesBackup) {
            // If there's a roles selection mechanism available, update it
            const filterSelect = document.querySelector('#filter select');
            if (filterSelect) {
                // @ts-ignore
                filterSelect.value = this.rolesBackup;
                filterSelect.dispatchEvent(new Event('change'));
            }
        }
        this.transition('viewing-network');
    },
    /**
     * Constructs the URL for fetching network data
     * @param {string} entityKey - Entity identifier in format 'type-id'
     * @returns {string} Formatted API URL with optional role parameters
     */
    getNetworkURL: function(entityKey) {
        var entityType = entityKey.split('-')[0];
        var entityId = entityKey.split('-')[1];
        var url = '/api/' + entityType + '/network/' + entityId;
        var roles = getSelectedRoles() || [];
        if (roles.length) {
            url += '?' + new URLSearchParams({ roles: roles.join(',') }).toString();
        }
        return url;
    },
    /**
     * Constructs URL for fetching random entity data
     * Includes random parameter to prevent caching
     * @returns {string} Formatted API URL for random entity
     */
    getRandomURL: function() {
        var url = '/api/random?r=' + Math.floor(Math.random() * 1000000);
        var roles = getSelectedRoles() || [];
        if (roles.length) {
            url += '&' + new URLSearchParams({ roles: roles.join(',') }).toString();
        }
        return url;
    },
    /**
     * Constructs URL for fetching radial view data
     * @param {string} entityKey - Entity identifier in format 'type-id'
     * @returns {string} Formatted API URL for radial relations
     */
    getRadialURL: function(entityKey) {
        var entityType = entityKey.split("-")[0];
        var entityId = entityKey.split("-")[1];
        return '/api/' + entityType+ '/relations/' + entityId;
    },
    /**
     * Loads initial data if available inline
     * Triggers data processing if dgNetwork is defined
     */
    loadInlineData: function() {
        // @ts-ignore
        if (dgNetwork) {
            // @ts-ignore
            this.handle('load-inline-data', dgNetwork);
        }
    },
    /**
     * Updates browser history state
     * Manages browser history and URL updates for navigation
     * @param {string} entityKey - Entity identifier
     * @param {Object} params - URL parameters
     */
    pushState: function(entityKey, params) {
        console.log("pushstate");
        var entityType = entityKey.split("-")[0];
        var entityId = entityKey.split("-")[1];
        var title = document.title;
        var url = "/" + entityType + "/" + entityId;
        if (params) {
            url += "?" + decodeURIComponent(new URLSearchParams(params).toString());
        }
        var state = {key: entityKey, params: params};
        window.history.pushState(state, title, url);
        // ### TODO setup analytics ga('send', 'pageview', url);
        // ### TODO setup analytics ga('set', 'page', url);
    },
    /**
     * Requests role data from the API
     * Transitions to requesting state and handles response
     */
    requestRoles: function() {
        console.log("requestRoles");
        this.transition('requesting');
        var self = this;
        d3.json(this.getRoles())
            .then(function(data) {
                self.handle('received-roles', data);
            })
            .catch(function(error) {
                self.handleError(error);
            });
    },
    /**
     * Requests network data for a specific entity
     * Transitions to requesting state and processes network data
     * @param {string} entityKey - Entity identifier
     * @param {boolean} pushHistory - Whether to update browser history
     */
    requestNetwork: function(entityKey, pushHistory) {
        console.log("requestNetwork key: ", entityKey);
        this.transition('requesting');
        var self = this;
        var url = this.getNetworkURL(entityKey);
        console.log("requestNetwork url: ", url);
        d3.json(url)
            .then(function(data) {
                self.handle('received-network', data, pushHistory);
            })
            .catch(function(error) {
                self.handleError(error);
            });
    },
    /**
     * Requests radial view data for a specific entity
     * Transitions to requesting state and processes radial data
     * @param {string} entityKey - Entity identifier
     */
    requestRadial: function(entityKey) {
        console.log("requestRadial: ", entityKey);
        this.transition('requesting');
        var self = this;
        var url = this.getRadialURL(entityKey);
        console.log("requestRadial url: ", url);
        d3.json(url)
            .then(function(data) {
                self.handle('received-radial', data);
            })
            .catch(function(error) {
                self.handleError(error);
            });
    },
    /**
     * Requests data for a random entity
     * Transitions to requesting state and fetches random entity
     */
    requestRandom: function() {
        this.transition('requesting');
        var self = this;
        var url = this.getRandomURL();
        console.log("requestRandom url: ", url);
        d3.json(url)
            .then(function(data) {
                self.handle('received-random', data);
            })
            .catch(function(error) {
                self.handleError(error);
            });
    },
    /**
     * Handles entity selection in the visualization
     * Triggers state update for entity selection
     * @param {string} entityKey - Entity identifier
     * @param {boolean} fixed - Whether to fix entity position
     */
    selectEntity: function(entityKey, fixed) {
        console.log("selectEntity: ", entityKey);
        this.handle('select-entity', entityKey, fixed);
    },
    /**
     * Transitions to network view
     * Triggers state change to viewing-network
     */
    showNetwork: function() {
        this.handle('show-network');
    },
    /**
     * Transitions to radial view
     * Triggers state change to viewing-radial
     */
    showRadial: function() {
        this.handle('show-radial');
    },
    /**
     * Toggles filter controls and tree interaction
     * @param {boolean} status - Enable/disable status
     */
    toggleFilter: function(status) {
        // Try to get the tree container
        const treeContainer = document.getElementById('jstree_div');
        if (!treeContainer) {
            console.warn('Tree container not found in DOM');
            return;  // Exit early if element doesn't exist
        }
        
        // Toggle interaction by adding/removing pointer-events style
        treeContainer.style.pointerEvents = status ? 'auto' : 'none';
        treeContainer.style.opacity = status ? '1' : '0.5';

        // Add visual feedback for disabled state
        if (!status) {
            treeContainer.setAttribute('aria-disabled', 'true');
            treeContainer.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
                /** @type {HTMLInputElement} */ (checkbox).disabled = true;
            });
        } else {
            treeContainer.removeAttribute('aria-disabled');
            treeContainer.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
                /** @type {HTMLInputElement} */ (checkbox).disabled = false;
            });
        }
    },
    /**
     * Toggles network visualization visibility and interaction
     * Manages opacity and force layout state
     * @param {boolean} status - Show/hide status
     */
    toggleNetwork: function(status) {
        console.log("toggleNetwork: ", status);
        if (status) {
            const root = dg.network.layers.root;
            if (root) {
                // @ts-ignore
                root.style.transition = 'opacity 250ms';
                // @ts-ignore
                root.style.opacity = 1;
            }
        } else {
            stopForceLayout();
            const root = dg.network.layers.root;
            if (root) {
                // @ts-ignore
                root.style.transition = 'opacity 250ms';
                // @ts-ignore
                root.style.opacity = 0.25;
            }
        }
    },
    /**
     * Toggles loading indicator visibility
     * Updates loading animation and data
     * @param {boolean} status - Show/hide status
     */
    toggleLoading: function(status) {
        console.log("toggleLoading: ", status);
        
        // Try to get the loading element
        let pageLoading = document.getElementById('page-loading');
        
        // If it doesn't exist, create it with Bootstrap spinner styling
        if (!pageLoading) {
            pageLoading = document.createElement('div');
            pageLoading.id = 'page-loading';
            pageLoading.className = 'position-fixed top-50 start-50 translate-middle';
            pageLoading.innerHTML = `
                <div class="spinner-border" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
            `;
            // Add any necessary styling
            pageLoading.style.zIndex = '1000';
            // Append to body
            document.body.appendChild(pageLoading);
        }
        
        // Update loading animation
        loading.toggle(status);
    },
    /**
     * Toggles radial view controls and icons
     * Updates UI elements for radial view state
     * @param {boolean} status - Enable/disable status
     */
    toggleRadial: function(status) {
        console.log("toggleRadial: ", status);
        const self = this;
        
        const entityRelations = document.getElementById('entity-relations');
        if (!entityRelations) {
            console.warn('entity-relations element not found in DOM');
            return;  // Exit early if element doesn't exist
        }   
    
        const entityRelationsIcon = entityRelations.querySelector('.bi');
        if (!entityRelationsIcon) {
            console.warn('entity-relations-icon element not found in DOM');
            return;  // Exit early if element doesn't exist
        }

        if (status) {
            entityRelations.removeEventListener('click', this._showNetworkHandler);
            entityRelations.addEventListener('click', this._showNetworkHandler = function(event) {
                    self.showNetwork();
                    event.preventDefault();
                });
            
            entityRelationsIcon.classList.remove('bi-eye-slash');
            entityRelationsIcon.classList.add('bi-eye');
        } else {
            entityRelations.removeEventListener('click', this._showNetworkHandler);
                entityRelations.addEventListener('click', this._showNetworkHandler = function(event) {
                    self.showRadial();
                    event.preventDefault();
                });

            entityRelationsIcon.classList.add('bi-eye-slash');
            entityRelationsIcon.classList.remove('bi-eye');
        }
    },
});