/**
 * DiscographFsm - A Finite State Machine implementation for the Discograph application
 * Built on machina.js FSM framework to manage application state and transitions
 * Handles network visualization, radial view transitions, and entity selection
 */

import { loading } from "./loading";
import { resetSvgSize, setSvgSize } from "./svg";
import {
    startForceLayout,
    restartForceLayout,
    stopForceLayout,
} from "./network/forceLayout";
import { debounce } from "./utils";
import type {
    NodeKey,
    NetworkCenter,
    NetworkData,
    SimData,
} from "./network/data";
import {
    processAPINetworkDataResponse,
    convertNetworkDataToSimData,
} from "./network/data";
import { pruneSimData } from "./network/pruning";
import type { RelationsData } from "./relations";
import { dg } from "./dg";
import { initWindow } from "./init";
import type { APINetworkDataResponse } from "./api";
import { fetchAPINetwork, fetchAPIRandom, fetchAPIRadial } from "./api";
import { resetNetworkTransform } from "./network/init";
import { ALPHA } from "./network/forceLayout";
import type { SimNode, SimLink } from "./network/data";
import { RequestNetworkEvent, SelectEntityEvent } from "./network/events";
import { showMessage } from "./messages";
import * as d3 from "d3";
import $ from "jquery";

interface FSMInstance {
    state: keyof FSMStates;
    handle(
        event: string,
        data: NetworkData | RelationsData | NetworkCenter | NodeKey | null,
        pushHistory: boolean,
        fixed: boolean,
    ): void;
    handleError(error: unknown): void;
    showNetwork(data: NetworkData, pushHistory: boolean): void;
    showRadial(data?: RelationsData): void;
    transition(state: keyof FSMStates): void;
    requestNetwork(entityKey: NodeKey, pushHistory: boolean): void;
    requestRandom(): void;
    requestRadial(entityKey: NodeKey): void;
    selectEntity(entityKey: NodeKey | null, fixed: boolean): void;
    loadInlineData(): void;
    toggleRadial(show: boolean): void;
    toggleNetwork(show: boolean): void;
    toggleLoading(show: boolean): void;
    toggleFilter(show: boolean): void;
    pushState(entityKey: string, params?: Record<string, unknown>): void;
    on(
        event: string,
        handler: (
            event: string,
            data: NetworkData | RelationsData | NetworkCenter | NodeKey | null,
        ) => void,
    ): void;
    _showNetworkHandler?: (event: Event) => void;
}

interface FSMState {
    _onEnter?: (this: FSMInstance) => void;
    _onExit?: (this: FSMInstance) => void;
    "received-network"?: (
        this: FSMInstance,
        data: NetworkData,
        pushHistory?: boolean,
    ) => void;
    "received-radial"?: (this: FSMInstance, data: RelationsData) => void;
    "request-network"?: (this: FSMInstance, entityKey: string) => void;
    "request-random"?: (this: FSMInstance) => void;
    "show-radial"?: (this: FSMInstance) => void;
    "show-network"?: (this: FSMInstance) => void;
    "select-entity"?: (
        this: FSMInstance,
        entityKey: string | null,
        fixed: boolean,
    ) => void;
    errored?: (this: FSMInstance, error: unknown) => void;
    handleError?: (this: FSMInstance, error: unknown) => void;
}

interface FSMStates {
    "state-requesting-network": FSMState;
    "state-requesting-radial": FSMState;
    "state-requesting-random": FSMState;
    "state-viewing-network": FSMState;
    "state-viewing-radial": FSMState;
    uninitialized: FSMState;
}

interface FSMConfig extends FSMState {
    initialize?: (this: FSMInstance) => void;
    namespace?: string;
    initialState?: keyof FSMStates;
    states?: FSMStates;
    handleError?: (this: FSMInstance, error: unknown) => void;
    loadInlineData?: (this: FSMInstance) => void;
    toggleRadial?: (this: FSMInstance, show: boolean) => void;
    toggleNetwork?: (this: FSMInstance, show: boolean) => void;
    toggleLoading?: (this: FSMInstance, show: boolean) => void;
    toggleFilter?: (this: FSMInstance, show: boolean) => void;
    pushState?: (this: FSMInstance, entityKey: string) => void;
    requestNetwork?: (
        this: FSMInstance,
        entityKey: NodeKey,
        pushHistory?: boolean,
    ) => void;
    requestRandom?: (this: FSMInstance) => void;
    requestRadial?: (this: FSMInstance, entityKey: NodeKey) => void;
    selectEntity?: (
        this: FSMInstance,
        entityKey: string | null,
        fixed: boolean,
    ) => void;
    showNetwork?: (
        this: FSMInstance,
        data?: NetworkData,
        pushHistory?: boolean,
    ) => void;
    showRadial?: (this: FSMInstance, data?: RelationsData) => void;
    handle?: (
        this: FSMInstance,
        event: string,
        data: NetworkData | RelationsData | NodeKey | null,
        pushHistory: boolean,
        fixed: boolean,
    ) => void;
    transition?: (this: FSMInstance, state: keyof FSMStates) => void;
}

declare global {
    interface Window {
        machina: {
            Fsm: {
                extend(config: FSMConfig): new () => FSMInstance;
            };
        };
        dgNetwork?: APINetworkDataResponse;
        jQuery: typeof $;
        $: typeof $;
    }
}

export const DiscographFsm = window.machina.Fsm.extend({
    /**
     * Initializes the FSM with event listeners and initial state setup
     * Sets up window event handlers for network requests, entity selection,
     * view transitions, and browser history management
     */
    initialize: function (this: FSMInstance) {
        // Event handlers
        window.addEventListener(
            "discograph:request-network",
            (event: Event) => {
                if (event instanceof RequestNetworkEvent && event.detail) {
                    const { entityKey, pushHistory } = event.detail;
                    this.requestNetwork(entityKey, pushHistory);
                }
            },
        );

        window.addEventListener("discograph:request-random", () => {
            this.requestRandom();
        });

        window.addEventListener("discograph:select-entity", (event: Event) => {
            if (event instanceof SelectEntityEvent && event.detail) {
                const { entityKey, fixed } = event.detail;
                this.selectEntity(entityKey, fixed);
            }
        });

        window.addEventListener("discograph:show-network", () => {
            this.showNetwork(null, false);
        });

        window.addEventListener("discograph:show-radial", () => {
            this.showRadial();
        });

        // Handle browser history navigation
        window.onpopstate = (event: PopStateEvent) => {
            const state = event?.state as { key: string } | null;
            if (state?.key) {
                window.dispatchEvent(new RequestNetworkEvent(state.key, false));
            }
        };

        // Handle window resize events with debounce
        const handleResize = debounce(() => {
            window.location.reload();
            resetSvgSize();
            initWindow();
            setSvgSize();

            // Center the visualization
            const transform = `translate(${dg.dimensions[0] / 2},${dg.dimensions[1] / 2})`;
            d3.selectAll(".centered")
                .transition()
                .duration(250)
                .attr("transform", transform);

            // Restart force layout if in network view
            if (this.state === "state-viewing-network") {
                restartForceLayout(ALPHA / 10.0);
            }
        }, 50);

        window.addEventListener("resize", handleResize);

        // Handle SVG mousedown events
        const svgDocument = document.getElementById("svg");
        if (svgDocument) {
            svgDocument.addEventListener("mousedown", () => {
                if (this.state === "state-viewing-network") {
                    this.selectEntity(null, false);
                } else if (this.state === "state-viewing-radial") {
                    this.showNetwork(null, false);
                }
            });
        }

        // Log all FSM events for debugging
        this.on(
            "*",
            (
                event: string,
                data: NetworkData | RelationsData | NodeKey | null,
            ) => {
                console.log("FSM: ", event, data);
            },
        );

        // Initialize application state
        this.loadInlineData();
        this.toggleRadial(false);
        //         this.rolesBackup = getSelectedRoles() || [];
    },

    namespace: "discograph",
    initialState: "uninitialized" as const,

    states: {
        "state-viewing-network": {
            _onEnter: function (this: FSMInstance) {
                console.log("VIEWING-NETWORK _onEnter");
                this.toggleNetwork(true);
                this.toggleRadial(false);
                this.toggleFilter(true);
            },
            _onExit: function (this: FSMInstance) {
                console.log("VIEWING-NETWORK _onExit");
                this.toggleNetwork(false);
                this.toggleFilter(false);
            },
            "request-network": function (
                this: FSMInstance,
                entityKey: NodeKey,
            ) {
                console.log("VIEWING-NETWORK request-network");
                this.requestNetwork(entityKey, true);
            },
            "request-random": function (this: FSMInstance) {
                console.log("VIEWING-NETWORK request-random");
                this.requestRandom();
            },
            "show-radial": function (this: FSMInstance) {
                console.log("VIEWING-NETWORK show-radial");
                this.showRadial();
            },
            "select-entity": function (
                this: FSMInstance,
                entityKey: NodeKey | null,
                fixed: boolean,
            ) {
                console.log("VIEWING-NETWORK select-entity:", entityKey);
                if (entityKey) {
                    this.selectEntity(entityKey, fixed);
                }
            },
        },

        "state-requesting-network": {
            _onEnter: function (this: FSMInstance) {
                console.log("REQUESTING-NETWORK _onEnter");
                this.toggleLoading(true);
            },
            _onExit: function (this: FSMInstance) {
                console.log("REQUESTING-NETWORK _onExit");
                this.toggleLoading(false);
            },
            errored: function (this: FSMInstance, error: unknown) {
                this.handleError(error);
            },
            "received-network": function (
                this: FSMInstance,
                data: NetworkData,
                pushHistory: boolean,
            ) {
                console.log("REQUESTING-NETWORK received-network data: ", data);
                this.showNetwork(data, pushHistory);
            },
            "received-random": function (
                this: FSMInstance,
                data: NetworkCenter,
            ) {
                console.log("REQUESTING-NETWORK received-random data: ", data);
                this.requestNetwork(data.center, true);
            },
            "received-radial": function (
                this: FSMInstance,
                data: RelationsData,
            ) {
                console.log("REQUESTING-NETWORK received-radial");
                this.showRadial(data);
            },
        },

        "state-requesting-radial": {
            _onEnter: function (this: FSMInstance) {
                this.toggleLoading(true);
            },
            _onExit: function (this: FSMInstance) {
                this.toggleLoading(false);
            },
            errored: function (this: FSMInstance, error: unknown) {
                this.handleError(error);
            },
            "received-radial": function (
                this: FSMInstance,
                data: RelationsData,
            ) {
                this.showRadial(data);
            },
        },

        "state-requesting-random": {
            _onEnter: function (this: FSMInstance) {
                console.log("REQUESTING-RANDOM _onEnter");
                this.toggleLoading(true);
            },
            _onExit: function (this: FSMInstance) {
                console.log("REQUESTING-RANDOM _onExit");
                this.toggleLoading(false);
            },
            errored: function (this: FSMInstance, error: unknown) {
                this.handleError(error);
            },
            "received-network": function (
                this: FSMInstance,
                data: NetworkData,
                pushHistory,
            ) {
                console.log("REQUESTING-RANDOM received-network");
                // this.data = data;
                this.showNetwork(data, pushHistory);
            },
        },

        "state-viewing-radial": {
            _onEnter: function (this: FSMInstance) {
                this.toggleNetwork(false);
                this.toggleRadial(true);
                this.toggleFilter(false);
            },
            _onExit: function (this: FSMInstance) {
                this.toggleRadial(false);
            },
            "request-network": function (
                this: FSMInstance,
                entityKey: NodeKey,
            ) {
                this.requestNetwork(entityKey, false);
            },
            "request-random": function (this: FSMInstance) {
                this.requestRandom();
            },
        },

        uninitialized: {
            _onEnter: function (this: FSMInstance) {
                console.log("UNITIALIZED _onEnter");
                this.loadInlineData();
            },
            _onExit: function (this: FSMInstance) {
                console.log("UNITIALIZED _onExit");
            },
            "received-network": function (
                this: FSMInstance,
                data: NetworkData,
                _pushHistory = false,
            ) {
                console.log("UNITIALIZED received-network");
                // this.data = data;
                this.transition("state-viewing-network");
            },
            "request-network": function (
                this: FSMInstance,
                entityKey: NodeKey,
            ) {
                console.log("UNITIALIZED request-network");
                this.transition("state-requesting-network");
                this.requestNetwork(entityKey, true);
            },
            "request-random": function (this: FSMInstance) {
                console.log("UNITIALIZED request-random");
                this.transition("state-requesting-random");
                this.requestRandom();
            },
        },
    } as FSMStates,

    handleError: function (this: FSMInstance, error: Error) {
        showMessage(error.message, "error");

        this.transition("state-viewing-network");
    },

    loadInlineData: function (this: FSMInstance) {
        if (window.dgNetwork) {
            this.transition("state-requesting-network");

            const networkData = processAPINetworkDataResponse(window.dgNetwork);

            this.handle("received-network", networkData, false, false);
        }
    },

    pushState: function (
        this: FSMInstance,
        entityKey: NodeKey,
        params?: Record<string, unknown>,
    ) {
        const [entityType, entityId] = entityKey.split("-");
        const title = document.title;
        let url = `/${entityType}/${entityId}`;

        if (params) {
            const searchParams = new URLSearchParams();
            Object.entries(params).forEach(([key, value]) => {
                if (Array.isArray(value)) {
                    searchParams.set(key, value.join(","));
                } else if (typeof value === "object" && value !== null) {
                    // Convert objects to a stable JSON string
                    const sorted = Object.fromEntries(
                        Object.entries(value).sort(([a], [b]) =>
                            a.localeCompare(b),
                        ),
                    );
                    searchParams.set(key, JSON.stringify(sorted));
                } else if (
                    typeof value === "string" ||
                    typeof value === "number" ||
                    typeof value === "boolean"
                ) {
                    searchParams.set(key, String(value));
                }
            });
            url += `?${decodeURIComponent(searchParams.toString())}`;
        }

        const state = { key: entityKey, params };
        window.history.pushState(state, title, url);
    },

    requestNetwork: function (
        this: FSMInstance,
        entityKey: NodeKey,
        pushHistory: boolean,
    ) {
        this.transition("state-requesting-network");

        fetchAPINetwork(entityKey)
            .then((apiNetworkDataResponse: APINetworkDataResponse) => {
                const networkData = processAPINetworkDataResponse(
                    apiNetworkDataResponse,
                );

                this.handle(
                    "received-network",
                    networkData,
                    pushHistory,
                    false,
                );
            })
            .catch((error) => {
                console.error("Error fetching network data:", error);

                this.handleError(error);
            });
    },

    requestRadial: function (this: FSMInstance, entityKey: string) {
        this.transition("state-requesting-radial" as keyof FSMStates);

        fetchAPIRadial(entityKey)
            .then((relationsData: RelationsData) => {
                this.handle("received-radial", relationsData, false, false);
            })
            .catch((error) => {
                console.error("Error fetching radial data:", error);

                this.handleError(error);
            });
    },

    requestRandom: function (this: FSMInstance) {
        this.transition("state-requesting-network");

        fetchAPIRandom()
            .then((networkCenter: NetworkCenter) => {
                this.handle("received-random", networkCenter, true, false);
            })
            .catch((error) => {
                console.error("Error fetching random data:", error);

                this.handleError(error);
            });
    },

    showNetwork: function (
        this: FSMInstance,
        networkData: NetworkData,
        pushHistory: boolean,
    ) {
        console.log("showNetwork networkData: ", networkData);

        this.transition("state-viewing-network");

        const filterValue = $("#filter select").val();
        const params = { roles: Array.isArray(filterValue) ? filterValue : [] };

        if (!networkData.center?.key) {
            console.error("Invalid network data: missing center key");
            return;
        }

        // Update the network data
        document.title = "Discograph2: " + networkData.center.name;
        $(document.body).attr("id", networkData.center.key);

        if (pushHistory) {
            this.pushState(networkData.center.key, params);
        }

        console.log("received-network convertNetworkDataToSimData");
        const simData: SimData = convertNetworkDataToSimData(networkData);

        const prunedSimData: SimData = pruneSimData(simData);
        dg.network.data = prunedSimData;

        console.log("received-network resetNetworkTransform");
        resetNetworkTransform();

        console.log("received-network startForceLayout");
        startForceLayout();

        this.handle("select-entity", networkData.center.key, false, false);
    },

    showRadial: function (this: FSMInstance) {
        this.transition("state-viewing-radial");
        this.handle("show-radial", undefined, false, false);
    },

    toggleFilter: function (this: FSMInstance, status: boolean) {
        const treeContainer = document.getElementById("jstree_div");
        if (!treeContainer) {
            console.warn("Tree container not found in DOM");
            return;
        }

        treeContainer.style.pointerEvents = status ? "auto" : "none";
        treeContainer.style.opacity = status ? "1" : "0.5";

        if (!status) {
            treeContainer.setAttribute("aria-disabled", "true");
            treeContainer
                .querySelectorAll('input[type="checkbox"]')
                .forEach((checkbox) => {
                    if (checkbox instanceof HTMLInputElement) {
                        checkbox.disabled = true;
                    }
                });
        } else {
            treeContainer.removeAttribute("aria-disabled");
            treeContainer
                .querySelectorAll('input[type="checkbox"]')
                .forEach((checkbox) => {
                    if (checkbox instanceof HTMLInputElement) {
                        checkbox.disabled = false;
                    }
                });
        }
    },

    toggleNetwork: function (this: FSMInstance, status: boolean) {
        if (status) {
            const root = dg.network.layers.root;
            if (root) {
                root.style("transition", "opacity 250ms").style("opacity", 1);
            }
        } else {
            stopForceLayout();
            const root = dg.network.layers.root;
            if (root) {
                root.style("transition", "opacity 250ms").style(
                    "opacity",
                    0.25,
                );
            }
        }
    },

    toggleLoading: function (this: FSMInstance, status: boolean) {
        loading.toggle(status);
    },

    toggleRadial: function (this: FSMInstance, status: boolean) {
        const entityRelations = document.getElementById("entity-relations");
        if (!entityRelations) {
            console.warn("entity-relations element not found in DOM");
            return;
        }

        //     const entityRelationsIcon = entityRelations.querySelector(".bi");
        //     if (!entityRelationsIcon) {
        //       console.warn("entity-relations-icon element not found in DOM");
        //       return;
        //     }

        if (status) {
            if (this._showNetworkHandler) {
                entityRelations.removeEventListener(
                    "click",
                    this._showNetworkHandler,
                );
            }

            this._showNetworkHandler = (event: Event) => {
                this.showNetwork(undefined, false);
                event.preventDefault();
            };

            entityRelations.addEventListener("click", this._showNetworkHandler);
            //       entityRelationsIcon.classList.remove("bi-eye-slash");
            //       entityRelationsIcon.classList.add("bi-eye");
        } else {
            if (this._showNetworkHandler) {
                entityRelations.removeEventListener(
                    "click",
                    this._showNetworkHandler,
                );
            }

            this._showNetworkHandler = (event: Event) => {
                this.showRadial(undefined);
                event.preventDefault();
            };

            entityRelations.addEventListener("click", this._showNetworkHandler);
            //       entityRelationsIcon.classList.add("bi-eye-slash");
            //       entityRelationsIcon.classList.remove("bi-eye");
        }
    },

    selectEntity: function (
        this: FSMInstance,
        entityKey: string | null,
        fixed: boolean,
    ) {
        console.log("selectEntity", entityKey, fixed);
        dg.selectedNodeKey = entityKey;
        let nodeOn: d3.Selection<SVGGElement, SimNode, SVGGElement, unknown>;
        let nodeOff: d3.Selection<SVGGElement, SimNode, SVGGElement, unknown>;
        let _linkOn: d3.Selection<SVGGElement, SimLink, SVGGElement, unknown>;
        let _linkOff: d3.Selection<SVGGElement, SimLink, SVGGElement, unknown>;

        if (entityKey !== null) {
            const root = dg.network.layers.root;
            if (!root) return;

            nodeOn = root.selectAll<SVGGElement, SimNode>(
                "g" + "#" + entityKey,
            );
            nodeOff = root.selectAll<SVGGElement, SimNode>(
                "g.node:not(#" + entityKey + ")",
            );

            const nodeData = nodeOn.datum();
            if (!nodeData) return;

            console.log("nodeData: ", nodeData);
            const linkKeys = nodeData.links.map((l) => l.key);
            const linkSelection = dg.network.selections.link;

            _linkOn = linkSelection.filter((d: SimLink) =>
                linkKeys.includes(d.key),
            );
            _linkOff = linkSelection.filter(
                (d: SimLink) => !linkKeys.includes(d.key),
            );

            const node = dg.network.data.nodeMap.get(entityKey);
            if (!node) return;

            const [, id] = node.key.split("-");
            const url = `http://discogs.com/${node.type}/${id}`;
            $("#entity-name").text(node.name);
            $("#entity-link").attr("href", url);
            $("#entity-details").removeClass("hidden").show(0);
            $("#navbar-title").text(node.name);
            nodeOn.raise();
            nodeOn.classed("selected", true);
            if (fixed) {
                //nodeOn.each(function(d) { d.fixed = true; });
                node.fixed = true;
            }
            // linkOn.classed('selected', true);
        } else {
            const root = dg.network.layers.root;
            if (!root) return;

            nodeOff = root.selectAll<SVGGElement, SimNode>("g.node");
            _linkOff = dg.network.selections.link;
        }

        if (nodeOff) {
            nodeOff.classed("selected", false).each(function (d) {
                d.fixed = false;
            });
        }
        if (_linkOff) {
            _linkOff.classed("selected", false);
        }
    },
}) as unknown as new () => FSMInstance;
