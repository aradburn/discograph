/**
 * Network Visualization Constants and Functions
 * This module provides functionality for rendering and manipulating network graphs,
 * particularly focused on node positioning, link rendering, and visual calculations.
 */

import * as d3 from 'd3';
import { hideTooltips } from './init';
import { dg } from '../dg';

// Constants
export const NODE_INNER_RADIUS = 8;
export const NODE_OUTER_RADIUS = 11;

// Array of roles that should not be labeled in the visualization
export const unlabeledRoles = [
    'Alias',
    'Member Of',
    'Sublabel Of',
];

/**
 * Calculates the base radius for a node based on its properties
 * @param {Object} d - The node data object
 * @param {number} d.distance - Distance from the center node
 * @param {number} d.size - Base size of the node
 * @param {Array} d.links - Array of node connections
 * @param {*} d.cluster - Cluster information (if any)
 * @returns {number} - Calculated radius for the node
 */
export const getRadius = (d) => {
    const boost1 = d.distance === 0 ? 10 : d.distance === 1 ? 5 : 0;
    const boost2 = d.links?.length >= 20 ? 10 : d.links?.length >= 10 ? 5 : 0;
    const alias = (d.cluster !== undefined) ? 2 : 1;
    return Math.round(((Math.sqrt(d.size) * 2) + boost1 + boost2) / alias);
};

/**
 * Calculates the outer radius of a node
 * @param {Object} d - The node data object
 * @returns {number} - Outer radius value
 */
export const getOuterRadius = (d) => NODE_OUTER_RADIUS + getRadius(d);

/**
 * Calculates the inner radius of a node
 * @param {Object} d - The node data object
 * @returns {number} - Inner radius value
 */
export const getInnerRadius = (d) => NODE_INNER_RADIUS + getRadius(d);

/**
 * Calculates spline intersection points for curved edges
 * @param {number} sX - Source X coordinate
 * @param {number} sY - Source Y coordinate
 * @param {number} sR - Source radius
 * @param {number} cX - Control point X coordinate
 * @param {number} cY - Control point Y coordinate
 * @returns {Array} - [x, y] coordinates of the intersection point
 */
export const calculateSplineInner = (sX, sY, sR, cX, cY) => {
    const dX = (sX - cX);
    const dY = (sY - cY);
    const angle = Math.atan(dY / dX);
    const deltaX = Math.abs(Math.cos(angle) * sR);
    const deltaY = Math.abs(Math.sin(angle) * sR);
    const newSX = (sX < cX) ? sX + deltaX : sX - deltaX;
    const newSY = (sY < cY) ? sY + deltaY : sY - deltaY;
    return [newSX, newSY];
};

/**
 * Generates SVG path data for edges between nodes
 * @param {Object} d - The edge data object
 * @returns {string} - SVG path data string
 */
export const generateSpline = (d) => {
    const { x: sX, y: sY, radius: sR } = d.source;
    const { x: tX, y: tY, radius: tR } = d.target;
    
    if (d.intermediate) {
        const { x: cX, y: cY } = d.intermediate;
        const [sXY0, sXY1] = calculateSplineInner(sX, sY, sR, cX, cY);
        const [tXY0, tXY1] = calculateSplineInner(tX, tY, tR, cX, cY);
        return `M ${sXY0},${sXY1} S ${cX},${cY} ${tXY0},${tXY1}`;
    }
    
    return `M ${sX},${sY} L ${tX},${tY}`;
};

/**
 * Calculates vertices for hull (outline) around node clusters
 * @param {Array} nodes - Array of nodes in the cluster
 * @returns {Array} - Array of vertex coordinates for hull calculation
 */
export const getHullVertices = (nodes) => {
    return nodes.flatMap(d => {
        const radius = d.radius / 3;
        return [
            [d.x + radius, d.y + radius],
            [d.x + radius, d.y - radius],
            [d.x - radius, d.y + radius],
            [d.x - radius, d.y - radius]
        ];
    });
};

/**
 * Updates link positions and labels during force simulation
 * @this {Element}
 * @param {Object} d - The link data object with source and target coordinates
 * @param {Object} d.source - Source node with coordinates
 * @param {number} d.source.x - X coordinate of source node
 * @param {number} d.source.y - Y coordinate of source node
 * @param {Object} d.target - Target node with coordinates
 * @param {number} d.target.x - X coordinate of target node
 * @param {number} d.target.y - Y coordinate of target node
 * @param {number} i - Index of the link
 */
const onTickLink = function(d, i) {
    const group = d3.select(this);
    const path = group.select('path');
    path.attr('d', generateSpline(d));
    
    const { x: x1, y: y1 } = d.source;
    const { x: x2, y: y2 } = d.target;
    const pathNode = path.node();
    /** @type {SVGPathElement|null} */
    const node = pathNode instanceof SVGPathElement ? pathNode : null;
    
    if (node && node.getTotalLength() > 0) {
        const point = node.getPointAtLength(node.getTotalLength() / 2);
        const angle = Math.atan2((y2 - y1), (x2 - x1)) * (180 / Math.PI);
        group.selectAll('text')
            .attr('transform', `rotate(${angle} ${point.x} ${point.y}) translate(${point.x},${point.y})`);
    }
};

/**
 * Helper function to generate transform attribute for node positioning
 * @param {Object} d - Node data object with x,y coordinates
 * @returns {string} - Transform attribute value
 */
const translate = (d) => `translate(${d.x},${d.y})`;

/**
 * Main tick function for force simulation
 * Updates positions of all visual elements (nodes, links, hulls) each tick
 * @param {Object} e - The tick event object
 */
export const onTick = (e) => {
    dg.network.tick += 1;
    const k = 1.0; // Force multiplier
    
    // Center the main node if not fixed
    if (dg.network.data.json) {
        const centerNode = dg.network.data.nodeMap.get(dg.network.data.json.center.key);
        if (!centerNode.fixed) {
            const dx = ((dg.svg_dimensions[0] / 2) - centerNode.x) * k;
            const dy = ((dg.svg_dimensions[1] / 2) - centerNode.y) * k;
            centerNode.x += dx;
            centerNode.y += dy;
        }
    }
    
    // Update positions of all visual elements
    /** @type {d3.Selection<Element, any, any, any>} */ 
    (dg.network.layers.link?.selectAll(".link"))?.each(onTickLink);
    dg.network.layers.halo?.selectAll(".node").attr('transform', translate);
    dg.network.layers.node?.selectAll(".node").attr('transform', translate);
    dg.network.layers.text?.selectAll(".node").attr('transform', translate);
    
    // Update hull (cluster outline) paths
    dg.network.layers.halo?.selectAll(".hull")
        .select('path')
        .attr('d', function(d) {
            const vertices = d3.polygonHull(getHullVertices(d.flat()));
            // @ts-ignore
            return 'M' + vertices.join('L') + 'Z';
        });
    
    hideTooltips();
};