/**
 * DiscographFsm - A Finite State Machine implementation for the Discograph application
 * Built on machina.js FSM framework to manage application state and transitions
 * Handles network visualization, radial view transitions, and entity selection
 */

import { loading } from "./loading";
import { getSelectedRoles } from "./roles";
import { resetSvgSize, setSvgSize } from "./svg";
import { restartForceLayout, stopForceLayout } from "./network/forceLayout";
import { debounce } from "./init";
import { dg } from "./dg";
import { initWindow } from "./init";
import { ALPHA } from "./network/forceLayout";
import { RequestNetworkEvent, SelectEntityEvent } from "./network/events";
import type { BaseNode } from "./network/node";
import * as d3 from "d3";

interface RawNetworkNode {
  key: string;
  name: string;
  type?: "label" | "artist";
  size?: number;
  x?: number;
  y?: number;
  distance?: number;
  radius?: number;
  links?: RawNetworkLink[];
  cluster?: number;
  fixed?: boolean;
  isIntermediate?: boolean;
  pages?: unknown;
}

interface RawNetworkLink {
  key: string;
  role: string;
  source: { key: string };
  target: { key: string };
  distance?: number;
  isSpline?: boolean;
  intermediate?: RawNetworkNode;
  pages?: unknown;
}

interface NetworkNodeWithKey extends BaseNode {
  key: string;
  name: string;
  type: "label" | "artist";
  size: number;
  x: number;
  y: number;
  distance: number;
  radius: number;
  links: NetworkLinkWithKey[];
  cluster?: number;
  fixed?: boolean;
  isIntermediate?: boolean;
  pages?: unknown;
}

interface NetworkLinkWithKey {
  key: string;
  role: string;
  source: NetworkNodeWithKey;
  target: NetworkNodeWithKey;
  distance?: number;
  isSpline?: boolean;
  intermediate?: NetworkNodeWithKey;
  pages?: unknown;
}

interface NetworkDataStore {
  nodeMap: Map<string, NetworkNodeWithKey>;
  nodes: NetworkNodeWithKey[];
  links: NetworkLinkWithKey[];
  center?: NetworkNodeWithKey;
  linkMap: Map<string, NetworkLinkWithKey>;
  maxDistance: number;
  pageCount: number;
  json: {
    center: {
      key: string;
      name: string;
    };
    nodes: NetworkNodeWithKey[];
    links: NetworkLinkWithKey[];
  };
}

interface RelationData {
  year: number;
  category: string;
  role: string;
}

type RelationsData = {
  results: RelationData[];
};

interface APIError {
  name: string;
  status: number;
  message: string;
}

interface NetworkPageData {
  selectedNodeKey: string | null;
  nodes: NetworkNodeWithKey[];
  links: NetworkLinkWithKey[];
}

interface FSMInstance {
  data: NetworkDataStore;
  state: keyof FSMStates;
  handle(
    event: string,
    data?: NetworkDataStore | RelationsData | string | null,
    pushHistory?: boolean,
    fixed?: boolean,
  ): void;
  handleError(error: unknown): void;
  showNetwork(entityKey?: string, pushHistory?: boolean): void;
  showRadial(data?: RelationsData): void;
  transition(state: keyof FSMStates): void;
  requestNetwork(entityKey: string, pushHistory?: boolean): void;
  requestRandom(): void;
  requestRadial(entityKey: string): void;
  selectEntity(entityKey: string | null, fixed: boolean): void;
  loadInlineData(): void;
  toggleRadial(show: boolean): void;
  toggleNetwork(show: boolean): void;
  toggleLoading(show: boolean): void;
  toggleFilter(show: boolean): void;
  on(
    event: string,
    handler: (
      event: string,
      data: NetworkDataStore | RelationsData | string | null,
    ) => void,
  ): void;
  getNetworkURL(entityKey: string): string;
  getRadialURL(entityKey: string): string;
  getRandomURL(): string;
  rolesBackup?: string[];
  _showNetworkHandler?: (event: Event) => void;
}

interface FSMState {
  _onEnter?: (this: FSMInstance) => void;
  _onExit?: (this: FSMInstance) => void;
  "received-network"?: (
    this: FSMInstance,
    data: NetworkDataStore,
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
  "requesting-network": FSMState;
  "requesting-radial": FSMState;
  "requesting-random": FSMState;
  "viewing-network": FSMState;
  "viewing-radial": FSMState;
  uninitialized: FSMState;
}

type D3NetworkSelection = d3.Selection<
  SVGGElement,
  NetworkNodeWithKey,
  SVGGElement,
  unknown
>;
type D3LinkSelection = d3.Selection<
  SVGGElement,
  NetworkLinkWithKey,
  SVGGElement,
  unknown
>;

interface NetworkLayers {
  root?: D3NetworkSelection;
}

interface NetworkSelections {
  link?: D3LinkSelection;
}

interface NetworkManager {
  pageData: NetworkPageData;
  layers: NetworkLayers;
  selections: NetworkSelections;
  data: NetworkDataStore;
  requestNetwork: (
    entityKey: string,
    params?: Record<string, string | number | boolean>,
  ) => void;
  requestRandom: () => void;
}

interface FSMConfig extends FSMState {
  initialize?: (this: FSMInstance) => void;
  namespace?: string;
  initialState?: keyof FSMStates;
  states?: FSMStates;
  getNetworkURL?: (this: FSMInstance, entityKey: string) => string;
  getRadialURL?: (this: FSMInstance, entityKey: string) => string;
  getRandomURL?: (this: FSMInstance) => string;
  handleError?: (this: FSMInstance, error: unknown) => void;
  loadInlineData?: (this: FSMInstance) => void;
  toggleRadial?: (this: FSMInstance, show: boolean) => void;
  toggleNetwork?: (this: FSMInstance, show: boolean) => void;
  toggleLoading?: (this: FSMInstance, show: boolean) => void;
  toggleFilter?: (this: FSMInstance, show: boolean) => void;
  pushState?: (this: FSMInstance, entityKey: string) => void;
  requestNetwork?: (
    this: FSMInstance,
    entityKey: string,
    pushHistory?: boolean,
  ) => void;
  requestRandom?: (this: FSMInstance) => void;
  requestRadial?: (this: FSMInstance, entityKey: string) => void;
  selectEntity?: (
    this: FSMInstance,
    entityKey: string | null,
    fixed: boolean,
  ) => void;
  showNetwork?: (
    this: FSMInstance,
    entityKey?: string,
    pushHistory?: boolean,
  ) => void;
  showRadial?: (this: FSMInstance, data?: RelationsData) => void;
  handle?: (
    this: FSMInstance,
    event: string,
    data?: NetworkDataStore | RelationsData | string | null,
    pushHistory?: boolean,
    fixed?: boolean,
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
    dgNetwork?: NetworkDataStore;
  }
  var dg: {
    network: NetworkManager;
    dimensions: [number, number];
  };
}

export const DiscographFsm = window.machina.Fsm.extend({
  /**
   * Initializes the FSM with event listeners and initial state setup
   * Sets up window event handlers for network requests, entity selection,
   * view transitions, and browser history management
   */
  initialize: function (this: FSMInstance) {
    this.data = {
      nodeMap: new Map(),
      nodes: [],
      links: [],
      linkMap: new Map(),
      maxDistance: 0,
      pageCount: 0,
      json: {
        center: { key: "", name: "" },
        nodes: [],
        links: [],
      },
    };
    // Event handlers
    window.addEventListener("discograph:request-network", (event: Event) => {
      if (event instanceof RequestNetworkEvent && event.detail) {
        const { entityKey, pushHistory } = event.detail;
        this.requestNetwork(entityKey, pushHistory);
      }
    });

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
      this.showNetwork();
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
      document.querySelectorAll(".centered").forEach((el) => {
        if (el instanceof HTMLElement) {
          el.style.transition = "transform 250ms";
          el.style.transform = transform;
        }
      });

      // Restart force layout if in network view
      if (this.state === "viewing-network") {
        restartForceLayout(ALPHA / 10.0);
      }
    }, 50);

    window.addEventListener("resize", handleResize);

    // Handle SVG mousedown events
    const svgDocument = document.getElementById("svg");
    if (svgDocument) {
      svgDocument.addEventListener("mousedown", () => {
        if (this.state === "viewing-network") {
          this.selectEntity(null, false);
        } else if (this.state === "viewing-radial") {
          this.showNetwork();
        }
      });
    }

    // Log all FSM events for debugging
    this.on(
      "*",
      (
        event: string,
        data: NetworkDataStore | RelationsData | string | null,
      ) => {
        console.log("FSM: ", event, data);
      },
    );

    // Initialize application state
    this.loadInlineData();
    this.toggleRadial(false);
    this.rolesBackup = getSelectedRoles() || [];
  },

  namespace: "discograph",
  initialState: "viewing-network" as const,

  states: {
    "viewing-network": {
      _onEnter: function (this: FSMInstance) {
        this.toggleNetwork(true);
        this.toggleRadial(false);
        this.toggleFilter(true);
      },
      _onExit: function (this: FSMInstance) {
        this.toggleNetwork(false);
        this.toggleFilter(false);
      },
      "request-network": function (this: FSMInstance, entityKey: string) {
        this.requestNetwork(entityKey);
      },
      "request-random": function (this: FSMInstance) {
        this.requestRandom();
      },
      "show-radial": function (this: FSMInstance) {
        this.transition("viewing-radial");
      },
      "select-entity": function (
        this: FSMInstance,
        entityKey: string | null,
        fixed: boolean,
      ) {
        if (entityKey) {
          this.selectEntity(entityKey, fixed);
        }
      },
    },

    "requesting-network": {
      _onEnter: function (this: FSMInstance) {
        this.toggleLoading(true);
      },
      _onExit: function (this: FSMInstance) {
        this.toggleLoading(false);
      },
      errored: function (this: FSMInstance, error: unknown) {
        this.handleError(error);
      },
      "received-network": function (
        this: FSMInstance,
        data: NetworkDataStore,
        pushHistory = true,
      ) {
        this.data = data;
        this.showNetwork(data.center?.key || "", pushHistory);
      },
      "received-random": function (this: FSMInstance, data: NetworkDataStore) {
        this.data = data;
        this.showNetwork(data.center?.key || "", false);
      },
      "received-radial": function (this: FSMInstance, data: RelationsData) {
        this.showRadial(data);
      },
    },

    "requesting-radial": {
      _onEnter: function (this: FSMInstance) {
        this.toggleLoading(true);
      },
      _onExit: function (this: FSMInstance) {
        this.toggleLoading(false);
      },
      errored: function (this: FSMInstance, error: unknown) {
        this.handleError(error);
      },
      "received-radial": function (this: FSMInstance, data: RelationsData) {
        this.showRadial(data);
        this.transition("viewing-radial");
      },
    },

    "requesting-random": {
      _onEnter: function (this: FSMInstance) {
        this.toggleLoading(true);
      },
      _onExit: function (this: FSMInstance) {
        this.toggleLoading(false);
      },
      errored: function (this: FSMInstance, error: unknown) {
        this.handleError(error);
      },
      "received-network": function (
        this: FSMInstance,
        data: NetworkDataStore,
        pushHistory = true,
      ) {
        this.data = data;
        this.showNetwork(data.center?.key || "", pushHistory);
      },
    },

    "viewing-radial": {
      _onEnter: function (this: FSMInstance) {
        this.toggleNetwork(false);
        this.toggleRadial(true);
        this.toggleFilter(false);
      },
      _onExit: function (this: FSMInstance) {
        this.toggleRadial(false);
      },
      "request-network": function (this: FSMInstance, entityKey: string) {
        this.requestNetwork(entityKey);
      },
      "request-random": function (this: FSMInstance) {
        this.requestRandom();
      },
      "show-network": function (this: FSMInstance) {
        this.transition("viewing-network");
      },
    },

    uninitialized: {
      _onEnter: function (this: FSMInstance) {
        this.loadInlineData();
      },
      "received-network": function (
        this: FSMInstance,
        data: NetworkDataStore,
        pushHistory = false,
      ) {
        this.data = data;
        this.transition("viewing-network");
      },
      "request-network": function (this: FSMInstance, entityKey: string) {
        this.transition("requesting-network");
        this.requestNetwork(entityKey);
      },
      "request-random": function (this: FSMInstance) {
        this.transition("requesting-random");
        this.requestRandom();
      },
    },
  } as FSMStates,

  handleError: function (this: FSMInstance, error: unknown) {
    let message = "Something went wrong!";
    const apiError = error as APIError;
    const status = apiError?.status || 404;

    if (status === 429) {
      message = "Hey, slow down, buddy. Give it a minute.";
    }

    const text = `
      <div class="alert alert-danger alert-dismissible" role="alert">
        <button type="button" class="close" data-dismiss="alert" aria-label="Close">
          <span aria-hidden="true">&times;</span>
        </button>
        <strong>${status}!</strong> ${message}
      </div>
    `;

    const flash = document.getElementById("flash");
    if (flash) {
      flash.innerHTML += text;
    }

    if (this.rolesBackup) {
      const filterSelect = document.querySelector("#filter select");
      if (filterSelect instanceof HTMLSelectElement) {
        filterSelect.value = this.rolesBackup.join(",");
        filterSelect.dispatchEvent(new Event("change"));
      }
    }
    this.transition("viewing-network");
  },

  getNetworkURL: function (this: FSMInstance, entityKey: string): string {
    const [entityType, entityId] = entityKey.split("-");
    let url = `/api/${entityType}/network/${entityId}`;
    const roles = getSelectedRoles() || [];
    if (roles.length) {
      url += `?${new URLSearchParams({ roles: roles.join(",") }).toString()}`;
    }
    return url;
  },

  getRandomURL: function (this: FSMInstance): string {
    let url = `/api/random?r=${Math.floor(Math.random() * 1000000)}`;
    const roles = getSelectedRoles() || [];
    if (roles.length) {
      url += `&${new URLSearchParams({ roles: roles.join(",") }).toString()}`;
    }
    return url;
  },

  getRadialURL: function (this: FSMInstance, entityKey: string): string {
    const [entityType, entityId] = entityKey.split("-");
    return `/api/${entityType}/relations/${entityId}`;
  },

  loadInlineData: function (this: FSMInstance) {
    if (window.dgNetwork) {
      this.handle("received-network", window.dgNetwork, false);
    }
  },

  pushState: function (
    this: FSMInstance,
    entityKey: string,
    params?: Record<string, string | number | boolean>,
  ) {
    const [entityType, entityId] = entityKey.split("-");
    const title = document.title;
    let url = `/${entityType}/${entityId}`;

    if (params) {
      const searchParams = new URLSearchParams();
      Object.entries(params).forEach(([key, value]) => {
        if (Array.isArray(value)) {
          searchParams.set(key, value.join(","));
        } else {
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
    entityKey: string,
    pushHistory = true,
  ) {
    this.transition("requesting-network");
    const url = this.getNetworkURL(entityKey);

    fetch(url)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json();
      })
      .then((rawData: { nodes: RawNetworkNode[]; links: RawNetworkLink[] }) => {
        if (
          !rawData ||
          !Array.isArray(rawData.nodes) ||
          !Array.isArray(rawData.links)
        ) {
          throw new Error("Invalid network data format");
        }

        // Process nodes and links
        const nodeMap = new Map<string, NetworkNodeWithKey>();
        const processedNodes = rawData.nodes.map((node) => {
          const processedNode: NetworkNodeWithKey = {
            key: node.key,
            name: node.name,
            type: node.type === "artist" ? "artist" : "label",
            size: typeof node.size === "number" ? node.size : 10,
            x: typeof node.x === "number" ? node.x : 0,
            y: typeof node.y === "number" ? node.y : 0,
            distance: typeof node.distance === "number" ? node.distance : 0,
            radius: typeof node.radius === "number" ? node.radius : 5,
            links: [],
            cluster: node.cluster,
            fixed: node.fixed,
            isIntermediate: node.isIntermediate,
            pages: node.pages,
          };
          nodeMap.set(node.key, processedNode);
          return processedNode;
        });

        const linkMap = new Map<string, NetworkLinkWithKey>();
        const processedLinks = rawData.links.map((link) => {
          const source = nodeMap.get(link.source.key);
          const target = nodeMap.get(link.target.key);
          if (!source || !target) {
            throw new Error("Invalid link: missing source or target node");
          }
          const processedLink: NetworkLinkWithKey = {
            key: link.key,
            role: link.role,
            source,
            target,
            distance: link.distance,
            isSpline: link.isSpline,
            pages: link.pages,
          };
          linkMap.set(link.key, processedLink);
          return processedLink;
        });

        // Update node links after all links are processed
        processedLinks.forEach((link) => {
          const source = nodeMap.get(link.source.key);
          const target = nodeMap.get(link.target.key);
          if (source && target) {
            source.links = source.links || [];
            target.links = target.links || [];
            source.links.push(link);
            target.links.push(link);
          }
        });

        const center = processedNodes.find((n) => n.key === entityKey);
        if (!center) {
          throw new Error("Center node not found");
        }

        const networkData: NetworkDataStore = {
          nodeMap,
          nodes: processedNodes,
          links: processedLinks,
          center,
          linkMap,
          maxDistance: Math.max(...processedNodes.map((n) => n.distance)),
          pageCount: 1,
          json: {
            center: { key: center.key, name: center.name },
            nodes: processedNodes,
            links: processedLinks,
          },
        };

        dg.network.data = networkData;
        this.handle("received-network", networkData, pushHistory);
      })
      .catch((error: unknown) => {
        console.error("Error fetching network data:", error);
        const apiError: APIError = {
          name: "APIError",
          status: error instanceof Error ? 500 : 404,
          message: error instanceof Error ? error.message : "Unknown error",
        };
        this.handleError(apiError);
      });
  },

  requestRadial: function (this: FSMInstance, entityKey: string) {
    this.transition("requesting-radial" as keyof FSMStates);
    const url = this.getRadialURL(entityKey);

    fetch(url)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json();
      })
      .then((data: RelationsData) => {
        if (!data || !Array.isArray(data.results)) {
          throw new Error("Invalid radial data format");
        }
        this.handle("received-radial", data);
      })
      .catch((error: Error) => {
        console.error("Error fetching radial data:", error);
        const apiError: APIError = {
          name: "APIError",
          status: error instanceof Error ? 500 : 404,
          message: error.message,
        };
        this.handleError(apiError);
      });
  },

  requestRandom: function (this: FSMInstance) {
    this.transition("requesting-network");
    const url = this.getRandomURL();

    d3.json<NetworkDataStore>(url)
      .then((data) => {
        if (data && "center" in data) {
          this.handle("received-random", data);
        }
      })
      .catch((error: unknown) => {
        this.handleError(error as APIError);
      });
  },

  showNetwork: function (this: FSMInstance) {
    this.handle("show-network");
  },

  showRadial: function (this: FSMInstance) {
    this.handle("show-radial");
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
        root.style("transition", "opacity 250ms").style("opacity", 0.25);
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

    const entityRelationsIcon = entityRelations.querySelector(".bi");
    if (!entityRelationsIcon) {
      console.warn("entity-relations-icon element not found in DOM");
      return;
    }

    if (status) {
      if (this._showNetworkHandler) {
        entityRelations.removeEventListener("click", this._showNetworkHandler);
      }

      this._showNetworkHandler = (event: Event) => {
        this.showNetwork(undefined, false);
        event.preventDefault();
      };

      entityRelations.addEventListener("click", this._showNetworkHandler);
      entityRelationsIcon.classList.remove("bi-eye-slash");
      entityRelationsIcon.classList.add("bi-eye");
    } else {
      if (this._showNetworkHandler) {
        entityRelations.removeEventListener("click", this._showNetworkHandler);
      }

      this._showNetworkHandler = (event: Event) => {
        this.showRadial(undefined);
        event.preventDefault();
      };

      entityRelations.addEventListener("click", this._showNetworkHandler);
      entityRelationsIcon.classList.add("bi-eye-slash");
      entityRelationsIcon.classList.remove("bi-eye");
    }
  },

  handle: function (this: FSMInstance, event: string, ...args: any[]) {
    if (event === "request-network") {
      this.handle("request-network", args[0] as string);
    } else if (event === "received-network") {
      this.handle(
        "received-network",
        args[0] as NetworkDataStore,
        args[1] as boolean,
        args[2] as boolean,
      );
    } else if (event === "received-radial") {
      this.handle("received-radial", args[0] as RelationsData);
    } else if (event === "select-entity") {
      this.handle(
        "select-entity",
        args[0] as string | null,
        args[1] as boolean,
      );
    } else {
      this.handle(event);
    }
  },

  selectEntity: function (
    this: FSMInstance,
    entityKey: string | null,
    fixed: boolean,
  ) {
    this.handle("select-entity", entityKey, fixed);
  },
}) as unknown as new () => FSMInstance;
