/**
 * Network Visualization Constants and Functions
 * This module provides functionality for rendering and manipulating network graphs,
 * particularly focused on node positioning, link rendering, and visual calculations.
 */

import * as d3 from "d3";
import { hideAllTooltips } from "./tooltips";
import { dg, networkStore } from "../dg";
import type { SimNode, SimLink } from "./data";

// Array of roles that should not be labeled in the visualization
export const unlabeledRoles = ["Alias", "Member Of", "Sublabel Of"];

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

/**
 * Generates SVG path data for edges between nodes
 * @param {SimLink} d - The edge data object
 * @returns {string} - SVG path data string
 */
export const generateSpline = (d: SimLink): string => {
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
 * @param {SimNode[]} nodes - Array of nodes in the cluster
 * @returns {[number, number][]} - Array of vertex coordinates for hull calculation
 */
export const getHullVertices = (nodes: SimNode[]): [number, number][] => {
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
 * @param {SimLink} d - The link data object with source and target coordinates
 * @param {number} i - Index of the link
 */
const onTickLink = function (this: Element, d: SimLink, _i: number): void {
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
 * @param {SimNode} d - Node data object with x,y coordinates
 * @returns {string} - Transform attribute value
 */
const translate = (d: SimNode): string => `translate(${d.x},${d.y})`;

/**
 * Main tick function for force simulation
 * Updates positions of all visual elements (nodes, links, hulls) each tick
 * @param {d3.Simulation<SimNode, undefined>} e - The tick event object
 */
export const onTick = (_e: d3.Simulation<SimNode, undefined>): void => {
    //     console.log("Tick", networkStore.tick);
    networkStore.tick += 1;
    const k = 1.0; // Force multiplier

    // Center the main node if not fixed
    if (networkStore.data.center) {
        const centerNode = networkStore.data.nodeMap.get(
            networkStore.data.center.key,
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
    networkStore.layers.link
        ?.selectAll<SVGGElement, SimLink>(".link")
        ?.each(onTickLink);
    networkStore.layers.halo?.selectAll(".node").attr("transform", translate);
    networkStore.layers.node?.selectAll(".node").attr("transform", translate);
    networkStore.layers.text?.selectAll(".node").attr("transform", translate);

    // Update hull (cluster outline) paths
    networkStore.layers.halo
        ?.selectAll(".hull")
        .select("path")
        .attr("d", function (d: SimNode[]) {
            const vertices = d3.polygonHull(getHullVertices(d));
            return vertices ? "M" + vertices.join("L") + "Z" : "";
        });

    hideAllTooltips();
};
