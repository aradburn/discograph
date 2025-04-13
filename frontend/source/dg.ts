/**
 * @fileoverview Core Discograph namespace and state management.
 * This module defines the main Discograph object that maintains application state.
 */

import * as d3 from "d3";
import type {
    NodeKey,
    LinkKey,
    SimData,
    SimNode,
    SimLink,
} from "./network/data";
import type { Relations, RelationsArcData } from "./relations";

/**
 * SVG layer containers for network visualization
 */
interface NetworkLayers {
    root: d3.Selection<SVGGElement, unknown, HTMLElement, unknown> | null;
    halo: d3.Selection<SVGGElement, unknown, HTMLElement, unknown> | null;
    text: d3.Selection<SVGGElement, unknown, HTMLElement, unknown> | null;
    node: d3.Selection<SVGGElement, unknown, HTMLElement, unknown> | null;
    link: d3.Selection<SVGGElement, unknown, HTMLElement, unknown> | null;
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
    data: SimData;
    /** SVG layer containers for different visual elements */
    layers: NetworkLayers;
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
    /** SVG container dimensions [width, height] */
    dimensions: [number, number];
    /** SVG dimensions [width, height] */
    svg_dimensions: [number, number];
    /** Currently selected node key */
    selectedNodeKey: NodeKey;
    /** D3 arc generator for relations visualization */
    arc: d3.Arc<RelationsArcData, RelationsArcData>;
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
    selectedNodeKey: null,
    arc: d3.arc<RelationsArcData, RelationsArcData>(),
};

export function getSelectedNodeKey(): NodeKey {
    return dg.selectedNodeKey;
}

/** Network state and functionality */
export const networkStore: Network = {
    dimensions: [0, 0],
    forceLayout: null,
    isUpdating: false,
    isRunningLayout: false,
    tick: 0,
    newNodeCoords: [0, 0],
    zoom: null,
    data: {
        center: {
            x: 0,
            y: 0,
            type: "artist",
            key: "",
            name: "",
            size: 0,
            missing: 0,
            hasMissing: false,
            distance: 0,
            radius: 0,
            lastClickTime: 0,
            lastTouchTime: 0,
            links: [],
            cluster: 0,
            fixed: false,
            isIntermediate: false,
        },
        nodeMap: new Map<NodeKey, SimNode>(),
        linkMap: new Map<LinkKey, SimLink>(),
        maxDistance: 0,
    } as SimData,
    layers: {
        root: null,
        halo: null,
        text: null,
        node: null,
        link: null,
    },
};

/** Relations state and functionality */
export const relationsStore: Relations = {
    data: {
        results: [],
    },
    byYear: new d3.InternMap(),
    byRole: new d3.InternMap(),
    layers: {
        root: null,
    },
};
