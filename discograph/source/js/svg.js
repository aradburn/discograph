/**
 * @fileoverview SVG manipulation utilities for Discograph
 * This module provides functionality for SVG initialization, sizing, definition setup,
 * and SVG export capabilities. It handles SVG element manipulation, styling, and
 * conversion to other image formats.
 */

import * as d3 from 'd3';
import { saveAs } from 'file-saver';
import { nodeToolTip } from './network/node';
import { linkToolTip } from './network/link';
import { dg } from './dg';
import { showMessage, clearMessages } from './init';

/**
 * Initializes the SVG element with basic setup
 * - Sets up window dimensions
 * - Creates SVG definitions (markers, gradients)
 * - Initializes tooltips for nodes and links
 */
export const initSvg = () => {
    // Setup window dimensions on SVG element
    setSvgSize();

    // Setup SVG common definitions
    setupSvgDefs();

    // Initialise tooltip
    d3.select('#svg')
        .call(nodeToolTip)
        .call(linkToolTip);
};

/**
 * Sets the size and viewport attributes of the main SVG element
 * Uses global dg.dimensions and dg.svg_dimensions for sizing
 */
export const setSvgSize = () => {
    // Setup window dimensions on SVG element
    d3.select("#svg")
        .attr("width", dg.dimensions[0])
        .attr("height", dg.dimensions[1])
        .attr("viewBox", `0 0 ${dg.svg_dimensions[0]} ${dg.svg_dimensions[1]}`)
        .attr("preserveAspectRatio", "none");
};

/**
 * Resets the SVG element size by removing explicit width/height CSS properties
 */
export const resetSvgSize = () => {
    const svg = document.querySelector("#svg");
    if (svg) {
        // @ts-ignore
        svg.style.cssText = '';
    }
};

/**
 * Sets up SVG definitions including:
 * - Arrowhead marker for directed connections
 * - Aggregate marker for relationship indicators
 * - Radial gradient for visual effects
 */
export const setupSvgDefs = () => {
    const defs = d3.select("#svg").append("defs");
    
    // ARROWHEAD
    defs.append("marker")
        .attr("id", "arrowhead")
        .attr("viewBox", "-5 -5 10 10")
        .attr("refX", 4)
        .attr("refY", 0)
        .attr("markerWidth", 5)
        .attr("markerHeight", 5)
        .attr("markerUnits", "strokeWidth")
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M 0,0 m -5,-5 L 5,0 L -5,5 L -2.5,0 L -5,-5 Z")
        .attr("stroke-linecap", "round")
        .attr("stroke-linejoin", "round");
    
    // AGGREGATE
    defs.append("marker")
        .attr("id", "aggregate")
        .attr("viewBox", "-5 -5 10 10")
        .attr("refX", 5)
        .attr("refY", 0)
        .attr("markerWidth", 5)
        .attr("markerHeight", 5)
        .attr("markerUnits", "strokeWidth")
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M 0,0 m 5,0 L 0,-3 L -5,0 L 0,3 L 5,0 Z")
        .attr("fill", "#fff")
        .attr("stroke", "#000")
        .attr("stroke-linecap", "round")
        .attr("stroke-linejoin", "round")
        .attr("stroke-width", 1.5);
    
    // RADIAL GRADIENT
    const gradient = defs.append('radialGradient')
        .attr('id', 'radial-gradient');
    
    const gradientStops = [
        { offset: '0%', color: '#333', opacity: '1.0' },
        { offset: '50%', color: '#333', opacity: '0.333' },
        { offset: '75%', color: '#333', opacity: '0.111' },
        { offset: '100%', color: '#333', opacity: '0.0' }
    ];

    gradientStops.forEach(({ offset, color, opacity }) => {
        gradient.append('stop')
            .attr('offset', offset)
            .attr('stop-color', color)
            .attr('stop-opacity', opacity);
    });
};

/**
 * Exports the SVG as a PNG image file
 * @param {number} width - The desired width of the output image
 * @param {number} height - The desired height of the output image
 */
export const printSvg = async (width, height) => {
    showMessage("info", "Saving image to disk, please wait...");
    const svgNode = d3.select("#svg").node();

    // @ts-ignore
    const svgString = getSvgString(svgNode);
    await svgString2Image(svgString, 2 * width, 2 * height, 'png', saveBlob);

    function saveBlob(dataBlob, filesize) {
        const entityKey = dg.network.pageData.selectedNodeKey;
        const node = dg.network.data.nodeMap.get(entityKey);
        saveAs(dataBlob, `Discograph2 - ${node.name}.png`);

        clearMessages(10);
        showMessage("success", "Saving image complete");
        clearMessages(10000);
    }
};

/**
 * Converts an SVG node to a string representation
 * @param {SVGElement} svgNode - The SVG DOM node to convert
 * @returns {string} The serialized SVG string with proper namespace handling
 */
const getSvgString = (svgNode) => {
    svgNode.setAttribute('xlink', 'http://www.w3.org/1999/xlink');
    const cssStyleText = getCSSStyles(svgNode);
    appendCSS(cssStyleText, svgNode);

    const serializer = new XMLSerializer();
    let svgString = serializer.serializeToString(svgNode);
    svgString = svgString.replace(/(\w+)?:?xlink=/g, 'xmlns:xlink='); // Fix root xlink without namespace
    svgString = svgString.replace(/NS\d+:href/g, 'xlink:href'); // Safari NS namespace fix

    return svgString;
};

/**
 * Gets CSS styles for SVG elements
 * @param {SVGElement} parentElement - The parent SVG element
 * @returns {string} Concatenated CSS rules
 */
const getCSSStyles = (parentElement) => {
    const selectorTextArr = new Set();
    const addSelector = (selector) => selectorTextArr.add(selector);

    // Add Parent element Id and Classes
    addSelector(`#${parentElement.id}`);
    Array.from(parentElement.classList).forEach(className => addSelector(`.${className}`));

    // Process all child nodes
    const nodes = parentElement.getElementsByTagName("*");
    Array.from(nodes).forEach(node => {
        if (node.id) addSelector(`#${node.id}`);
        
        Array.from(node.classList).forEach(className => {
            // Basic class
            addSelector(`.${className}`);
            
            // Node type with class
            if (node.nodeName) {
                addSelector(`${node.nodeName}.${className}`);
            }
            
            // Parent relationships
            const parentNode = /** @type {HTMLElement} */ (node.parentNode);
            if (parentNode) {
                if (parentNode.id) {
                    addSelector(`#${parentNode.id} .${className}`);
                    if (node.nodeName) {
                        addSelector(`#${parentNode.id} ${node.nodeName}.${className}`);
                    }
                }
                
                Array.from(parentNode.classList).forEach(parentClass => {
                    addSelector(`.${parentClass}`);
                    addSelector(`.${parentClass} .${className}`);
                    if (node.nodeName) {
                        addSelector(`.${parentClass} ${node.nodeName}.${className}`);
                    }
                });
                
                // Grandparent relationships
                const grandParentNode = /** @type {HTMLElement} */ (parentNode.parentNode);
                if (grandParentNode && grandParentNode.id) {
                    addSelector(`#${grandParentNode.id} .${className}`);
                    Array.from(parentNode.classList).forEach(parentClass => {
                        addSelector(`#${grandParentNode.id} .${parentClass}`);
                        addSelector(`#${grandParentNode.id} .${parentClass} .${className}`);
                        if (node.nodeName) {
                            addSelector(`#${grandParentNode.id} .${parentClass} ${node.nodeName}.${className}`);
                        }
                    });
                }
            }
        });
    });

    // Extract CSS Rules
    let extractedCSSText = "";
    for (const sheet of document.styleSheets) {
        try {
            const cssRules = sheet.cssRules;
            if (!cssRules) continue;

            for (const rule of cssRules) {
                if (rule instanceof CSSStyleRule && selectorTextArr.has(rule.selectorText)) {
                    extractedCSSText += rule.cssText + "\n";
                }
            }
        } catch (e) {
            if (e.name !== 'SecurityError') throw e; // for Firefox
            continue;
        }
    }

    return extractedCSSText;
};

/**
 * Appends CSS styles to an SVG element
 * @param {string} cssText - The CSS text to append
 * @param {SVGElement} element - The target SVG element
 */
const appendCSS = (cssText, element) => {
    const styleElement = document.createElement("style");
    styleElement.setAttribute("type", "text/css");
    styleElement.textContent = cssText;
    const refNode = element.firstChild || null;
    element.insertBefore(styleElement, refNode);
};

/**
 * Converts an SVG string to an image blob
 * @param {string} svgString - The SVG content as a string
 * @param {number} width - The desired width of the output image
 * @param {number} height - The desired height of the output image
 * @param {string} format - The desired output format (default: 'png')
 * @param {Function} callback - Callback function that receives the blob and filesize
 * @returns {Promise} A promise that resolves when the image is converted
 */
const svgString2Image = (svgString, width, height, format = 'png', callback) => {
    return new Promise((resolve) => {
        // Convert SVG string to data URL
        const imgsrc = 'data:image/svg+xml;base64,' + btoa(unescape(encodeURIComponent(svgString)));

        const canvas = document.createElement("canvas");
        const context = canvas.getContext("2d");

		if (canvas !== null && context !== null) {
			canvas.width = width;
			canvas.height = height;

			const image = new Image();
			image.onload = () => {
				context.clearRect(0, 0, width, height);
				context.drawImage(image, 0, 0, width, height);

				canvas.toBlob(blob => {
					// @ts-ignore
					const filesize = Math.round(blob.size / 1024) + ' KB';
					callback?.(blob, filesize);
					// @ts-ignore
					resolve();
				});
			};

			image.src = imgsrc;
		}
    });
};
