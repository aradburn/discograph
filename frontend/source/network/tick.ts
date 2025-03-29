/**
 * Network Visualization Constants and Functions
 * This module provides functionality for rendering and manipulating network graphs,
 * particularly focused on node positioning, link rendering, and visual calculations.
 */

import * as d3 from "d3";
import { hideAllTooltips } from "./tooltips";
import { dg } from "../dg";
import type { NetworkNode } from "./node";
import type { NetworkLink } from "./link";

// Constants
export const NODE_INNER_RADIUS = 8;
export const NODE_OUTER_RADIUS = 11;

// Array of roles that should not be labeled in the visualization
export const unlabeledRoles = ["Alias", "Member Of", "Sublabel Of"];

// Base interface for positioned elements
interface Positioned {
    x: number;
    y: number;
    radius: number;
}

// Extend NetworkNode with position information
type PositionedNode = NetworkNode &
    Positioned & {
        fixed?: boolean;
        cluster?: unknown;
        distance: number;
        size: number;
        links?: NetworkLink[];
    };

// Extend the DiscographCore interface in dg.ts instead of declaring it here
declare module "../dg" {
    interface DiscographCore {
        svg_dimensions: [number, number];
    }
}

/**
 * Calculates the base radius for a node based on its properties
 * @param {PositionedNode} d - The node data object
 * @returns {number} - Calculated radius for the node
 */
export const getRadius = (d: PositionedNode): number => {
    const boost1 = d.distance === 0 ? 10 : d.distance === 1 ? 5 : 0;
    const boost2 = d.links?.length >= 20 ? 10 : d.links?.length >= 10 ? 5 : 0;
    const alias = d.cluster !== undefined ? 2 : 1;
    return Math.round((Math.sqrt(d.size) * 2 + boost1 + boost2) / alias);
};

/**
 * Calculates the outer radius of a node
 * @param {PositionedNode} d - The node data object
 * @returns {number} - Outer radius value
 */
export const getOuterRadius = (d: PositionedNode): number =>
    NODE_OUTER_RADIUS + getRadius(d);

/**
 * Calculates the inner radius of a node
 * @param {PositionedNode} d - The node data object
 * @returns {number} - Inner radius value
 */
export const getInnerRadius = (d: PositionedNode): number =>
    NODE_INNER_RADIUS + getRadius(d);

/**
 * Calculates spline intersection points for curved edges
 * @param {number} sX - Source X coordinate
 * @param {number} sY - Source Y coordinate
 * @param {number} sR - Source radius
 * @param {number} cX - Control point X coordinate
 * @param {number} cY - Control point Y coordinate
 * @returns {[number, number]} - [x, y] coordinates of the intersection point
 */
export const calculateSplineInner = (
    sX: number,
    sY: number,
    sR: number,
    cX: number,
    cY: number,
): [number, number] => {
    const dX = sX - cX;
    const dY = sY - cY;
    const angle = Math.atan(dY / dX);
    const deltaX = Math.abs(Math.cos(angle) * sR);
    const deltaY = Math.abs(Math.sin(angle) * sR);
    const newSX = sX < cX ? sX + deltaX : sX - deltaX;
    const newSY = sY < cY ? sY + deltaY : sY - deltaY;
    return [newSX, newSY];
};

// Extend NetworkLink with positioned nodes
type PositionedLink = Omit<NetworkLink, "source" | "target"> & {
    source: PositionedNode;
    target: PositionedNode;
    intermediate?: {
        x: number;
        y: number;
    };
};

/**
 * Generates SVG path data for edges between nodes
 * @param {PositionedLink} d - The edge data object
 * @returns {string} - SVG path data string
 */
export const generateSpline = (d: PositionedLink): string => {
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
 * @param {PositionedNode[]} nodes - Array of nodes in the cluster
 * @returns {[number, number][]} - Array of vertex coordinates for hull calculation
 */
export const getHullVertices = (
    nodes: PositionedNode[],
): [number, number][] => {
    return nodes.flatMap((d) => {
        const radius = d.radius / 3;
        return [
            [d.x + radius, d.y + radius],
            [d.x + radius, d.y - radius],
            [d.x - radius, d.y + radius],
            [d.x - radius, d.y - radius],
        ] as [number, number][];
    });
};

/**
 * Updates link positions and labels during force simulation
 * @this {Element}
 * @param {PositionedLink} d - The link data object with source and target coordinates
 * @param {number} i - Index of the link
 */
const onTickLink = function (
    this: Element,
    d: PositionedLink,
    _i: number,
): void {
    const group = d3.select(this);
    const path = group.select("path");
    path.attr("d", generateSpline(d));

    const { x: x1, y: y1 } = d.source;
    const { x: x2, y: y2 } = d.target;
    const pathNode = path.node();
    const node = pathNode instanceof SVGPathElement ? pathNode : null;

    if (node && node.getTotalLength() > 0) {
        const point = node.getPointAtLength(node.getTotalLength() / 2);
        const angle = Math.atan2(y2 - y1, x2 - x1) * (180 / Math.PI);
        group
            .selectAll("text")
            .attr(
                "transform",
                `rotate(${angle} ${point.x} ${point.y}) translate(${point.x},${point.y})`,
            );
    }
};

/**
 * Helper function to generate transform attribute for node positioning
 * @param {PositionedNode} d - Node data object with x,y coordinates
 * @returns {string} - Transform attribute value
 */
const translate = (d: PositionedNode): string => `translate(${d.x},${d.y})`;

/**
 * Main tick function for force simulation
 * Updates positions of all visual elements (nodes, links, hulls) each tick
 * @param {d3.Simulation<PositionedNode, undefined>} e - The tick event object
 */
export const onTick = (_e: d3.Simulation<PositionedNode, undefined>): void => {
    //     console.log("Tick", dg.network.tick);
    dg.network.tick += 1;
    const k = 1.0; // Force multiplier

    // Center the main node if not fixed
    if (dg.network.data.json) {
        const centerNode = dg.network.data.nodeMap.get(
            dg.network.data.json.center.key,
        );
        if (centerNode && !centerNode.fixed) {
            const [svgWidth, svgHeight] = dg.svg_dimensions;
            const dx = (svgWidth / 2 - centerNode.x) * k;
            const dy = (svgHeight / 2 - centerNode.y) * k;
            centerNode.x += dx;
            centerNode.y += dy;
        }
    }

    // Update positions of all visual elements
    dg.network.layers.link
        ?.selectAll<SVGGElement, PositionedLink>(".link")
        ?.each(onTickLink);
    dg.network.layers.halo?.selectAll(".node").attr("transform", translate);
    dg.network.layers.node?.selectAll(".node").attr("transform", translate);
    dg.network.layers.text?.selectAll(".node").attr("transform", translate);

    // Update hull (cluster outline) paths
    dg.network.layers.halo
        ?.selectAll(".hull")
        .select("path")
        .attr("d", function (d: PositionedNode[]) {
            const vertices = d3.polygonHull(getHullVertices(d));
            return vertices ? "M" + vertices.join("L") + "Z" : "";
        });

    hideAllTooltips();
};
