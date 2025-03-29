/**
 * @fileoverview Core Discograph namespace and state management.
 * This module defines the main Discograph object that maintains application state.
 */

import * as d3 from "d3";
import type { SimNode, SimLink } from "./network/forceLayout";
import type { NetworkNode } from "./network/node";
import type { NetworkLink } from "./network/link";
import type { DiscographFsm } from "./fsm";

/**
 * Core network data structure
 */
interface NetworkData {
    json: { center: { key: string } } | null;
    nodeMap: Map<string, SimNode>;
    linkMap: Map<string, SimLink>;
    maxDistance: number;
    pageCount: number;
}

/**
 * Current page state and data
 */
interface PageData {
    currentPage: number;
    links: SimLink[];
    nodes: SimNode[];
    selectedNodeKey: string | null;
}

/**
 * D3 selections for network visualization elements
 */
interface NetworkSelections {
    halo: d3.Selection<SVGGElement, SimNode, SVGGElement, unknown> | null;
    hull: d3.Selection<SVGGElement, SimNode[], SVGGElement, unknown> | null;
    node: d3.Selection<SVGGElement, SimNode, SVGGElement, unknown> | null;
    link: d3.Selection<SVGGElement, SimLink, SVGGElement, unknown> | null;
    text: d3.Selection<SVGGElement, SimNode, SVGGElement, unknown> | null;
}

/**
 * SVG layer containers for network visualization
 */
interface NetworkLayers {
    root: d3.Selection<SVGGElement, SimNode | SimLink, SVGGElement, unknown> | null;
    halo: d3.Selection<SVGGElement, SimNode, SVGGElement, unknown> | null;
    text: d3.Selection<SVGGElement, SimNode, SVGGElement, unknown> | null;
    node: d3.Selection<SVGGElement, SimNode, SVGGElement, unknown> | null;
    link: d3.Selection<SVGGElement, SimLink, SVGGElement, unknown> | null;
}

/**
 * Data structure for individual relations
 */
interface RelationData {
    year: number;
    category: string;
    role: string;
}

/**
 * Collection of relation data
 */
interface RelationsData {
    results: RelationData[];
}

/**
 * SVG layer containers for relations visualization
 */
interface RelationsLayers {
    root: d3.Selection<
        SVGGElement,
        NetworkNode | NetworkLink,
        HTMLElement,
        unknown
    > | null;
}

/**
 * Relations state and functionality
 */
export interface Relations {
    data: RelationsData;
    byYear: d3.InternMap<number, d3.InternMap<string, RelationData[]>>;
    byRole: d3.InternMap<string, number>;
    layers: RelationsLayers;
}

/**
 * Network state and configuration
 */
export interface Network {
    /** Current dimensions [width, height] of the network visualization area */
    dimensions: [number, number];
    /** Force layout simulation */
    forceLayout: d3.Simulation<SimNode, SimLink>;
    /** Flag indicating if the network is currently being updated */
    isUpdating: boolean;
    /** Flag indicating if the force layout is currently running */
    isRunningLayout: boolean;
    /** Counter for animation/simulation ticks */
    tick: number;
    /** Coordinates [x, y] for placing new nodes */
    newNodeCoords: [number, number];
    /** Zoom behavior */
    zoom: d3.ZoomBehavior<SVGGElement, unknown> | null;
    /** Core data storage for the network */
    data: NetworkData;
    /** Current page state and selections */
    pageData: PageData;
    /** D3 selections for various visual elements */
    selections: NetworkSelections;
    /** SVG layer containers for different visual elements */
    layers: NetworkLayers;
}

/**
 * Arc data type for relations visualization
 */
interface ArcData {
    startAngle: number;
    endAngle: number;
    innerRadius: number;
    outerRadius: number;
    padAngle?: number;
}

/**
 * Core Discograph application state
 */
export interface DiscographCore {
    /** Version number */
    version: string;
    /** Debug mode flag */
    debug: boolean;
    /** Device pixel ratio */
    dpr: number;
    /** Container dimensions [width, height] */
    dimensions: [number, number];
    /** SVG dimensions [width, height] */
    svg_dimensions: [number, number];
    /** Network state and functionality */
    network: Network;
    /** Relations state and functionality */
    relations: Relations;
    /** Finite state machine instance */
    fsm: InstanceType<typeof DiscographFsm> | null;
    /** D3 arc generator for relations visualization */
    arc: d3.Arc<ArcData, ArcData>;
}

/**
 * Main Discograph namespace object
 * Contains core application functionality and version information
 */
export const dg: DiscographCore = {
    version: "2.1.0",
    debug: false,
    dpr: window.devicePixelRatio,
    dimensions: [0, 0],
    svg_dimensions: [0, 0],
    network: {
        dimensions: [0, 0],
        forceLayout: null,
        isUpdating: false,
        isRunningLayout: false,
        tick: 0,
        newNodeCoords: [0, 0],
        zoom: null,
        data: {
            json: null,
            nodeMap: new Map(),
            linkMap: new Map(),
            maxDistance: 0,
            pageCount: 1,
        },
        pageData: {
            currentPage: 1,
            links: [],
            nodes: [],
            selectedNodeKey: null,
        },
        selections: {
            halo: null,
            hull: null,
            node: null,
            link: null,
            text: null,
        },
        layers: {
            root: null,
            halo: null,
            text: null,
            node: null,
            link: null,
        },
    },
    relations: {
        data: {
            results: [],
        },
        byYear: new d3.InternMap(),
        byRole: new d3.InternMap(),
        layers: {
            root: null,
        },
    },
    fsm: null,
    arc: d3.arc<ArcData, ArcData>(),
};
