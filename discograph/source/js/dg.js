/**
 * @fileoverview Core Discograph namespace and state management.
 * This module defines the main Discograph object that maintains application state.
 * @module dg
 */

import * as d3 from 'd3';
/** @typedef {import('./network/forceLayout').NodeType} NodeType */

/**
 * Main Discograph namespace object
 * Contains core application functionality and version information
 */
export const dg = {
    version: "2.1.0",
    debug: false,
    svg_dimensions: [0, 0],
    network: {
        /** @property {number[]} dimensions - Current dimensions [width, height] of the network visualization area */
        dimensions: [0, 0],

        /** @type {d3.Simulation<NodeType, undefined>|null} */
        forceLayout: null,

        /** @property {boolean} isUpdating - Flag indicating if the network is currently being updated */
        isUpdating: false,

        /** @property {boolean} isRunningLayout - Flag indicating if the force layout is currently running */
        isRunningLayout: false,

        /** @property {number} tick - Counter for animation/simulation ticks */
        tick: 0,

        /** @property {number[]} newNodeCoords - Coordinates [x, y] for placing new nodes */
        newNodeCoords: [0, 0],

        /** @type {d3.ZoomBehavior<d3.BaseType, unknown> | null} */
        zoom: null,

        /** @property {Object} data - Core data storage for the network
         * @type {{
         *   json: { center: { key: string } } | null,
         *   nodeMap: Map<any, any>,
         *   linkMap: Map<any, any>,
         *   maxDistance: number,
         *   pageCount: number
         * }}
         */
        data: {
            json: null,
            nodeMap: new Map(),
            linkMap: new Map(),
            maxDistance: 0,
            pageCount: 1,
        },

        /** @property {Object} pageData - Current page state and selections
         * @type {{
         *   currentPage: number,
         *   links: any[],
         *   nodes: any[],
         *   selectedNodeKey: string | null
         * }}
         */
        pageData: {
            currentPage: 1,
            links: [],
            nodes: [],
            selectedNodeKey: null,
        },

        /** @property {Object} selections - D3 selections for various visual elements
         * @type {{
         *   halo: d3.Selection<SVGGElement, any, SVGGElement, unknown> | null,
         *   hull: d3.Selection<SVGGElement, any, SVGGElement, unknown> | null,
         *   node: d3.Selection<SVGGElement, any, SVGGElement, unknown> | null,
         *   link: d3.Selection<SVGGElement, any, SVGGElement, unknown> | null,
         *   text: d3.Selection<SVGGElement, any, SVGGElement, unknown> | null
         * }}
         */
        selections: {
            halo: null,
            hull: null,
            node: null,
            link: null,
            text: null,
        },

        /** @property {Object} layers - SVG layer containers for different visual elements
         * @type {{
         *   root: d3.Selection<SVGGElement, unknown, HTMLElement, any> | null,
         *   halo: d3.Selection<SVGGElement, unknown, HTMLElement, any> | null,
         *   text: d3.Selection<SVGGElement, unknown, HTMLElement, any> | null,
         *   node: d3.Selection<SVGGElement, unknown, HTMLElement, any> | null,
         *   link: d3.Selection<SVGGElement, unknown, HTMLElement, any> | null
         * }}
         */
        layers: {
            root: null,
            halo: null,
            text: null,
            node: null,
            link: null,
        },
    },
    /**
     * Relations state object that maintains the layers for the visualization
     */
    relations: {
        layers: {
            /**
             * @type {d3.Selection<SVGGElement, any, HTMLElement, any> | null}
             */
            root: null,
        },
    },
}; 